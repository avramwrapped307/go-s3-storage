package templates

import (
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/url"
	"time"
)

//go:embed layouts/*.html pages/*.html
var TemplateFS embed.FS

// PageTemplates holds templates for each page
type PageTemplates struct {
	templates map[string]*template.Template
	funcMap   template.FuncMap
}

// LoadTemplates parses all HTML templates and returns a template collection
func LoadTemplates() (*PageTemplates, error) {
	funcMap := template.FuncMap{
		"formatBytes":   FormatBytes,
		"formatDate":    FormatDate,
		"truncateHash":  TruncateHash,
		"urlEncode":     url.PathEscape,     // URL-encode paths for use in URLs (encodes # as %23, space as %20, etc.)
		"queryEncode":   url.QueryEscape,    // URL-encode query parameters
		"add":           func(a, b int) int { return a + b },
		"sub":           func(a, b int) int { return a - b },
		"subtract":      func(a, b int) int { return a - b },
		"mul":           func(a, b int) int { return a * b },
		"div": func(a, b int) int {
			if b == 0 {
				return 0
			}
			return a / b
		},
		"percentUsed": func(used, total int64) int {
			if total <= 0 {
				return 0
			}
			pct := (used * 100) / total
			if pct > 100 {
				return 100
			}
			return int(pct)
		},
		"buildPaginationURL": BuildPaginationURL,
		"hasRole": func(roles []string, role string) bool {
			if roles == nil {
				return false
			}
			for _, r := range roles {
				if r == role {
					return true
				}
			}
			return false
		},
		"truncate": func(s string, max int) string {
			if len(s) <= max {
				return s
			}
			return s[:max] + "..."
		},
		"firstChar": func(s string) string {
			if len(s) == 0 {
				return "?"
			}
			return string([]rune(s)[0])
		},
		"safeSlice": func(s string, start, end int) string {
			runes := []rune(s)
			if len(runes) == 0 {
				return ""
			}
			if start >= len(runes) {
				return ""
			}
			if end > len(runes) {
				end = len(runes)
			}
			return string(runes[start:end])
		},
	}

	pt := &PageTemplates{
		templates: make(map[string]*template.Template),
		funcMap:   funcMap,
	}

	// Read base layout
	baseContent, err := TemplateFS.ReadFile("layouts/base.html")
	if err != nil {
		return nil, fmt.Errorf("failed to read base layout: %w", err)
	}

	// List all page templates
	pages, err := fs.ReadDir(TemplateFS, "pages")
	if err != nil {
		return nil, fmt.Errorf("failed to read pages directory: %w", err)
	}

	for _, page := range pages {
		if page.IsDir() {
			continue
		}
		pageName := page.Name()
		if len(pageName) < 6 || pageName[len(pageName)-5:] != ".html" {
			continue
		}

		// Read page content
		pageContent, err := TemplateFS.ReadFile("pages/" + pageName)
		if err != nil {
			return nil, fmt.Errorf("failed to read page %s: %w", pageName, err)
		}

		// Create a new template for this page combining base + page
		tmplName := pageName[:len(pageName)-5] // Remove .html
		tmpl := template.New(tmplName).Funcs(funcMap)

		// Parse base layout first
		tmpl, err = tmpl.Parse(string(baseContent))
		if err != nil {
			return nil, fmt.Errorf("failed to parse base for %s: %w", pageName, err)
		}

		// Parse page content
		tmpl, err = tmpl.Parse(string(pageContent))
		if err != nil {
			return nil, fmt.Errorf("failed to parse page %s: %w", pageName, err)
		}

		pt.templates[tmplName] = tmpl
	}

	return pt, nil
}

// ExecuteTemplate renders a page template
func (pt *PageTemplates) ExecuteTemplate(w interface{ Write([]byte) (int, error) }, name string, data interface{}) error {
	// Special case for login page which doesn't use base
	if name == "login" {
		tmpl, ok := pt.templates["login"]
		if !ok {
			return fmt.Errorf("template %s not found", name)
		}
		return tmpl.ExecuteTemplate(w, "login", data)
	}

	tmpl, ok := pt.templates[name]
	if !ok {
		return fmt.Errorf("template %s not found", name)
	}

	return tmpl.ExecuteTemplate(w, "base", data)
}

// FormatBytes converts bytes to human-readable format
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// FormatDate converts Unix timestamp to formatted date string
func FormatDate(timestamp int64) string {
	if timestamp == 0 {
		return "-"
	}
	t := time.Unix(timestamp, 0)
	return t.Format("Jan 02, 2006 15:04:05")
}

// BuildPaginationURL builds a URL for pagination with all query parameters preserved
func BuildPaginationURL(bucket, prefix string, page, perPage int, sortBy, sortOrder, search string) string {
	baseURL := fmt.Sprintf("/ui/buckets/%s/objects", bucket)

	params := url.Values{}
	if prefix != "" {
		params.Set("prefix", prefix)
	}
	params.Set("page", fmt.Sprintf("%d", page))
	params.Set("per_page", fmt.Sprintf("%d", perPage))
	if sortBy != "" && sortBy != "name" {
		params.Set("sort_by", sortBy)
	}
	if sortOrder != "" && sortOrder != "asc" {
		params.Set("sort_order", sortOrder)
	}
	if search != "" {
		params.Set("search", search)
	}

	if len(params) > 0 {
		return baseURL + "?" + params.Encode()
	}
	return baseURL
}

// TruncateHash truncates a SHA256 hash for display
// Shows first 8 characters + "..." + last 4 characters (e.g., "a1b2c3d4...xyz9")
func TruncateHash(hash string) string {
	if hash == "" {
		return "-"
	}
	// SHA256 hashes are 64 characters (32 bytes in hex)
	if len(hash) <= 16 {
		return hash
	}
	return hash[:8] + "..." + hash[len(hash)-4:]
}
