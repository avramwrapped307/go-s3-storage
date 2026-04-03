package api

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/danbordeanu/go-logger"
	"github.com/danbordeanu/go-stats/concurrency"
	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"s3-storage/api/handlers"
	"s3-storage/api/handlers/ui"
	"s3-storage/api/middleware"
	"s3-storage/assets"
	"s3-storage/auth"
	"s3-storage/configuration"
	"s3-storage/services"
	"s3-storage/templates"
)

const httpServerShutdownGracePeriodSeconds = 20

// StartGin initializes and starts the Gin HTTP server, and blocks until a shutdown signal is received.
// It sets up all routes, middleware, and handlers for both the S3 API and the optional Web UI, and ensures a graceful shutdown when the context is cancelled.
// This function is designed to be run as a goroutine and will return once the HTTP server has been shutdown gracefully or the shutdown timeout has been reached.
// The function performs the following steps:
// 1. Initializes the Gin router and configures middleware for logging, recovery, CORS, rate limiting, memory management, and Prometheus metrics.
// 2. Sets up S3 API routes for bucket and object operations, as well as share link management.
// 3. If the Web UI is enabled, it initializes HTML templates, authentication providers, and UI handlers, and registers the corresponding routes.
// 4. Starts the HTTP server in a separate goroutine and listens for incoming requests.
// 5. Blocks until the provided context is cancelled (e.g., on SIGTERM/SIGINT), then attempts to shutdown the HTTP server gracefully within a specified timeout period.
func StartGin(ctx context.Context) {
	defer concurrency.GlobalWaitGroup.Done()

	conf := configuration.AppConfig()
	log := logger.SugaredLogger()

	// Set up gin
	log.Debugf("Setting up Gin")
	if !conf.GinLogger {
		gin.SetMode(gin.ReleaseMode)
	}
	router := gin.New()

	// setup pprof
	if conf.PprofEnabled {
		log.Debugf("Pprof is active, enabling endpoints")
		pprof.Register(router)
	}

	// Set up the middleware
	if conf.GinLogger {
		log.Warnf("Gin's logger is active! Logs will be unstructured!")
		router.Use(gin.Logger())
	}

	router.Use(gin.Recovery())
	router.Use(middleware.CorrelationId())
	// limiter
	router.Use(middleware.LimiterMiddleware(int(conf.MaxEventsPerSec), int(conf.MaxBurstSize)))
	// gc
	router.Use(middleware.ManageMemoryMiddleware())

	// metrics
	registry := middleware.NewRegister()
	router.Use(middleware.PrometheusMiddleware(configuration.OTName))

	// TODO: We can move CORS to Ingress
	if conf.CorsAllowOrigins != "Disabled" {
		router.Use(cors.New(cors.Config{
			AllowOrigins: []string{conf.CorsAllowOrigins},
			AllowMethods: []string{"POST", "HEAD", "PATCH", "OPTIONS", "GET", "PUT"},
			AllowHeaders: []string{"Content-Type", "Content-Length", "Accept-Encoding", "X-CSRF-Token",
				"Authorization", "accept", "origin", "Cache-Control", "X-Requested-With"},
			ExposeHeaders:    []string{"Content-Length"},
			AllowCredentials: true,
			MaxAge:           12 * time.Hour,
		}))
	}
	router.Use(otelgin.Middleware("s3-server"))

	// Set MaxMultipartMemory to 32MB (adjust as needed)
	router.MaxMultipartMemory = 32 << 20 // 32MB

	// metrics
	router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(registry, promhttp.HandlerOpts{})))

	// S3 API routes at root level for boto3 compatibility
	s3API := router.Group("")

	// Initialize session store early if WebUI is enabled (needed for S3 API routes)
	var sessionStore *auth.SessionStore
	if conf.WebUIEnabled {
		sessionStore = auth.NewSessionStore(conf.SessionTTL)
		// Apply optional session auth to S3 API routes so UI users can use S3 endpoints
		s3API.Use(middleware.OptionalSessionAuth(sessionStore))
	}

	// Create credential store and user service (needed for S3 auth and Web UI)
	credStore := auth.NewMemoryStore(conf.S3AccessKeyID, conf.S3SecretAccessKey)

	// Initialize local auth provider and user service
	localAuth := auth.NewLocalProvider(nil, conf.LocalAuthUsername, conf.LocalAuthPassword)
	bootstrapUser := localAuth.GetBootstrapUser()

	userService, err := services.NewUserService(conf.StorageDirectory, bootstrapUser, credStore)
	if err != nil {
		log.Fatalf("Failed to initialize user service: %s", err.Error())
	}

	// Update local auth to use user service for lookups
	localAuth = auth.NewLocalProvider(userService.GetByUsername, conf.LocalAuthUsername, conf.LocalAuthPassword)

	// Link bootstrap admin to global credentials if both exist
	if conf.S3AccessKeyID != "" && bootstrapUser != nil {
		bootstrapUser.S3AccessKeyID = conf.S3AccessKeyID
		bootstrapUser.S3SecretAccessKey = conf.S3SecretAccessKey
	}

	// Set up SigV4 authentication if enabled
	if conf.S3AuthEnabled {
		log.Infof("S3 authentication is enabled for region: %s", conf.S3AuthRegion)
		s3API.Use(middleware.SigV4Auth(credStore, conf.S3AuthRegion, userService))
	}

	// Initialize metastore
	metaStore, err := services.NewMetaStore(conf.StorageDirectory)
	if err != nil {
		log.Fatalf("Failed to initialize metastore: %s", err.Error())
	}

	// Initialize bucket service
	services.InitBucketService(metaStore, conf.StorageDirectory)

	// Initialize share link manager
	if err = services.InitShareLinkManager(conf.StorageDirectory); err != nil {
		log.Fatalf("Failed to initialize share link manager: %s", err.Error())
	}

	// Initialize stats service (needed for /api/stats routes which must be registered before S3 routes)
	statsService := services.NewStatsService(metaStore)

	// Register public share route (no authentication required)
	router.GET("/share/:token", handlers.GetSharedObject)

	// Activate swagger BEFORE S3 routes to avoid routing conflicts
	// (otherwise /:bucket/*key would match /swagger/doc.json as bucket="swagger")
	if conf.UseSwagger {
		log.Infof("Swagger is active, enabling endpoints")
		url := ginSwagger.URL("/swagger/doc.json")
		router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, url))
	}

	// Handle common browser requests BEFORE S3 wildcard routes
	// (otherwise /:bucket or /:bucket/*key would catch these)
	router.GET("/favicon.ico", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	router.GET("/robots.txt", func(c *gin.Context) {
		c.String(http.StatusOK, "User-agent: *\nDisallow: /")
	})

	// Serve static assets BEFORE S3 routes to prevent /ui/static/* being matched as /:bucket/*key
	staticFS, _ := fs.Sub(assets.StaticFS, ".")
	router.StaticFS("/ui/static", http.FS(staticFS))

	// Register /api/* routes BEFORE S3 wildcard routes to avoid routing conflicts
	// (otherwise /:bucket/*key would match /api/stats as bucket="api", key="/stats")
	apiGroup := router.Group("/api")
	{
		apiGroup.GET("/stats", func(c *gin.Context) {
			if !conf.WebUIEnabled {
				c.JSON(http.StatusNotFound, gin.H{"error": "Web UI is not enabled"})
				return
			}
			user := middleware.GetUserFromContext(c)
			stats := statsService.GetStatsForUser(user)
			c.JSON(http.StatusOK, stats)
		})
		apiGroup.GET("/stats/:bucket", func(c *gin.Context) {
			if !conf.WebUIEnabled {
				c.JSON(http.StatusNotFound, gin.H{"error": "Web UI is not enabled"})
				return
			}
			user := middleware.GetUserFromContext(c)
			bucketName := c.Param("bucket")
			if !services.CanAccessBucket(user, bucketName, false, metaStore) {
				c.JSON(http.StatusForbidden, gin.H{"error": "access denied"})
				return
			}
			stats, err := statsService.GetBucketStats(bucketName)
			if err != nil {
				c.JSON(http.StatusNotFound, gin.H{"error": "bucket not found"})
				return
			}
			c.JSON(http.StatusOK, stats)
		})
	}

	// Register bucket routes (S3-compatible paths)
	s3API.GET("/", handlers.ListBuckets)
	s3API.PUT("/:bucket", handlers.CreateBucket)
	s3API.DELETE("/:bucket", handlers.DeleteBucket)
	s3API.HEAD("/:bucket", handlers.HeadBucket)
	s3API.GET("/:bucket", handlers.ListObjects)

	// Register multipart upload dispatcher middleware
	// This middleware intercepts multipart upload requests and routes them to the appropriate handlers
	// It must be registered BEFORE the object routes to take precedence
	s3API.Use(multipartDispatcher())

	// Register object routes
	s3API.PUT("/:bucket/*key", handlers.PutObject)
	s3API.GET("/:bucket/*key", handlers.GetObject)
	s3API.HEAD("/:bucket/*key", handlers.HeadObject)
	s3API.DELETE("/:bucket/*key", handlers.DeleteObject)
	// POST route for multipart uploads (handled by multipartDispatcher middleware)
	s3API.POST("/:bucket/*key", func(c *gin.Context) {
		// This handler should never be reached because multipartDispatcher intercepts all POST requests
		// If we get here, it means the request didn't match any multipart upload patterns
		c.JSON(http.StatusNotFound, gin.H{"error": "Not Found"})
	})

	// Register share link management routes (protected by auth if enabled)
	s3API.POST("/share/create/:bucket/*key", handlers.CreateShareLink)
	s3API.DELETE("/share/:token", handlers.DeleteShareLink)

	// ============================================
	// Web UI Setup (if enabled)
	// ============================================
	if conf.WebUIEnabled {
		log.Infof("Web UI is enabled at %s", conf.WebUIPrefix)

		// Load HTML templates
		tmpl, err := templates.LoadTemplates()
		if err != nil {
			log.Fatalf("Failed to load templates: %s", err.Error())
		}

		// Session store, user service, and local auth were already initialized above

		// Initialize UI handlers
		authHandler := ui.NewAuthHandler(tmpl, localAuth, sessionStore)
		dashboardHandler := ui.NewDashboardHandler(tmpl, statsService, metaStore)
		bucketsHandler := ui.NewBucketsHandler(tmpl, userService, metaStore)
		objectsHandler := ui.NewObjectsHandler(tmpl, metaStore)
		sharesHandler := ui.NewSharesHandler(tmpl)
		usersHandler := ui.NewUsersHandler(tmpl, userService)

		// Public UI routes (no auth required)
		router.GET("/ui/login", authHandler.LoginPage)
		router.POST("/ui/login", authHandler.Login)

		// Protected UI routes
		uiGroup := router.Group("/ui")
		uiGroup.Use(middleware.SessionAuth(sessionStore))
		uiGroup.Use(middleware.CSRFProtection(sessionStore))
		{
			uiGroup.POST("/logout", authHandler.Logout)
			uiGroup.GET("/dashboard", dashboardHandler.Dashboard)
			uiGroup.GET("/buckets", bucketsHandler.BucketsPage)
			uiGroup.POST("/buckets", bucketsHandler.CreateBucket)
			uiGroup.DELETE("/buckets/:bucket", bucketsHandler.DeleteBucket)
			uiGroup.GET("/buckets/:bucket/objects", objectsHandler.ObjectsPage)
			uiGroup.GET("/shares", sharesHandler.SharesPage)

			// Bucket permissions management (admin only)
			uiGroup.GET("/buckets/:bucket/permissions", middleware.RequireRole("admin"), bucketsHandler.BucketPermissionsPage)
			uiGroup.PUT("/buckets/:bucket/permissions", middleware.RequireRole("admin"), bucketsHandler.SetBucketPermission)

			// User management (admin only)
			uiGroup.GET("/users", middleware.RequireRole("admin"), usersHandler.UsersPage)
			uiGroup.POST("/users", middleware.RequireRole("admin"), usersHandler.CreateUser)
			uiGroup.PUT("/users/:id", middleware.RequireRole("admin"), usersHandler.UpdateUser)
			uiGroup.DELETE("/users/:id", middleware.RequireRole("admin"), usersHandler.DeleteUser)

			// S3 credentials management (admin only)
			uiGroup.PUT("/users/:id/s3-credentials", middleware.RequireRole("admin"), usersHandler.SetS3Credentials)

			// Self-service routes (order matters: /me before /:id)
			uiGroup.PUT("/users/me/password", usersHandler.ChangeOwnPassword)
			uiGroup.PUT("/users/me/s3-credentials", usersHandler.SetOwnS3Credentials)

			// Admin-only user management routes
			uiGroup.PUT("/users/:id/password", middleware.RequireRole("admin"), usersHandler.AdminResetPassword)
		}

		// Redirect root to dashboard when UI is enabled
		router.GET("/ui", func(c *gin.Context) {
			c.Redirect(http.StatusFound, "/ui/dashboard")
		})
	}

	// Set up the listener
	httpSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", conf.HttpPort),
		Handler: router,
	}

	// Start the HTTP Server
	go func() {
		log.Infof("Listening on port %d", conf.HttpPort)
		if err = httpSrv.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("Unrecoverable HTTP Server failure: %s", err.Error())
			}
		}
	}()

	// Start background cleanup for expired multipart uploads
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := services.CleanupExpiredUploads(context.Background()); err != nil {
					log.Errorf("Multipart cleanup failed: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Block until SIGTERM/SIGINT
	<-ctx.Done()

	// Clean up and shutdown the HTTP server
	cleanCtx, cancel := context.WithTimeout(context.Background(), httpServerShutdownGracePeriodSeconds*time.Second)
	defer cancel()
	log.Infof("Attempting to shutdown the HTTP server with a timeout of %d seconds", httpServerShutdownGracePeriodSeconds)
	if err = httpSrv.Shutdown(cleanCtx); err != nil {
		log.Errorf("HTTP server failed to shutdown gracefully: %s", err.Error())
	} else {
		log.Infof("HTTP Server was shutdown successfully")
	}
}
