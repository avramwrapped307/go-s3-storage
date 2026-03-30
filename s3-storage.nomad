variable "telemetry_jaegerEndpoint" {
  type = string
  default = ""
}

variable "ingress_prefix" {
  type    = string
  default = ""
}


variable "request_base_url" {
  type = string
  default = "https://s3-storage.myhost.example.com"
}

variable "s3_auth_enabled" {
  type    = string
  default = "true"
}


variable  "s3_access_key_id" {
  type    = string
  default = "accesskeyid"
}

variable  "s3_secret_access_key" {
  type    = string
  default = "secretkeyid"
}

variable "s3_region" {
  type    = string
  default = "us-east-1"
}

variable "web_ui_enabled" {
  type    = string
  default = "true"
}

variable "local_auth_enabled" {
  type    = string
  default = "true"
}

variable "local_auth_username" {
  type    = string
  default = "admin"
}

variable "local_auth_password" {
  type    = string
  default = "mysupersecretpassword"
}

variable "storage_quota_bytes" {
  type    = number
  default = 10737418240 # 10240 MB (matches ephemeral_disk size)
}


job "go-s3-storage" {
  datacenters = ["dc1"]
  type = "service"

  group "go-s3-storage" {
    # ephemeral_disk {
    #   size    = 10240 # MB (10GB)
    #   sticky  = true
    #   migrate = true
    # }

    network {
      mode = "cni/nomad-cni0"
      port "http" {
        to     = 8080
      }
      dns {
        servers = ["172.17.0.1", "1.1.1.1"]
      }
    }

    volume "s3_storage" {
      type      = "host"
      source    = "s3_storage"
      read_only = false
    }

    task "go-s3-storage" {
      driver = "docker"
      config {
        image = "s3-storage:local"
        ports = ["http"]
        # volumes = [
        #   "local/data:/data"
        # ]
      }

      volume_mount {
        volume      = "s3_storage"
        destination = "/data"
        read_only   = false
      }


      env {

        JAEGER_ENDPOINT = "${var.telemetry_jaegerEndpoint}"

        REQUEST_BASE_URL = "${var.request_base_url}"
        INGRESS_PREFIX = "${var.ingress_prefix}"

        S3_AUTH_ENABLED = "${var.s3_auth_enabled}"
        S3_ACCESS_KEY_ID = "${var.s3_access_key_id}"
        S3_SECRET_ACCESS_KEY = "${var.s3_secret_access_key}"
        S3_REGION = "${var.s3_region}"

        WEB_UI_ENABLED = "${var.web_ui_enabled}"
        LOCAL_AUTH_ENABLED = "${var.local_auth_enabled}"
        LOCAL_AUTH_USERNAME = "${var.local_auth_username}"
        LOCAL_AUTH_PASSWORD = "${var.local_auth_password}"
        STORAGE_QUOTA_BYTES = "${var.storage_quota_bytes}"
      }

      resources {
        cpu    = 2024
        memory = 4024
      }
    }

    service {
      provider = "consul"
      name = "go-s3-storage"
      port = "http"
      address_mode = "alloc"
      tags = [
        "traefik.enable=true",

        # Main router - handles all paths (S3 API at root, UI, share, etc.)
        "traefik.http.routers.gos3.rule=Host(`s3-storage.myhost.example.com`)",
        "traefik.http.routers.gos3.entrypoints=https",
        "traefik.http.routers.gos3.tls.certresolver=myresolver",

        # HTTP -> HTTPS redirect router
        "traefik.http.routers.gos3-http.rule=Host(`s3-storage.myhost.example.com`)",
        "traefik.http.routers.gos3-http.entrypoints=http",
        "traefik.http.routers.gos3-http.middlewares=gos3-redirect-https",

        # Middleware to redirect HTTP to HTTPS
        "traefik.http.middlewares.gos3-redirect-https.redirectscheme.scheme=https",
        "metrics"
      ]
    }
  }
}
