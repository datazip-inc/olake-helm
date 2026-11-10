package docker

import (
	"strings"
	"testing"

	"github.com/moby/moby/api/types/mount"
)

func TestParseJobBindMounts(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []mount.Mount
		wantErr string
	}{
		{
			name: "empty input",
		},
		{
			name:  "whitespace only",
			input: "  \n ",
		},
		{
			name:  "read only",
			input: "/opt/certs:/etc/ssl/custom-certs",
			want:  []mount.Mount{{Type: mount.TypeBind, Source: "/opt/certs", Target: "/etc/ssl/custom-certs", ReadOnly: true}},
		},
		{
			name:  "target is cleaned",
			input: "/opt/certs:/etc//ssl/./certs/",
			want:  []mount.Mount{{Type: mount.TypeBind, Source: "/opt/certs", Target: "/etc/ssl/certs", ReadOnly: true}},
		},
		{
			name:  "multiple mounts",
			input: "/a:/x, /b:/y",
			want: []mount.Mount{
				{Type: mount.TypeBind, Source: "/a", Target: "/x", ReadOnly: true},
				{Type: mount.TypeBind, Source: "/b", Target: "/y", ReadOnly: true},
			},
		},
		{
			name:  "path next to the config dir",
			input: "/a:/mnt/configs",
			want:  []mount.Mount{{Type: mount.TypeBind, Source: "/a", Target: "/mnt/configs", ReadOnly: true}},
		},
		{
			name:    "missing separator",
			input:   "/a",
			wantErr: "host:container format",
		},
		{
			name:    "too many parts",
			input:   "/a:/x:ro",
			wantErr: "host:container format",
		},
		{
			name:    "empty entry",
			input:   "/a:/x,",
			wantErr: "host:container format",
		},
		{
			name:    "relative source",
			input:   "certs:/x",
			wantErr: "absolute host path",
		},
		{
			name:    "missing source",
			input:   ":/x",
			wantErr: "absolute host path",
		},
		{
			name:    "relative target",
			input:   "/a:x",
			wantErr: "absolute container path",
		},
		{
			name:    "missing target",
			input:   "/a:",
			wantErr: "absolute container path",
		},
		{
			name:    "target is the container root",
			input:   "/a:/",
			wantErr: "container root",
		},
		{
			name:    "target is the config dir",
			input:   "/a:/mnt/config",
			wantErr: "shadow",
		},
		{
			name:    "target inside the config dir",
			input:   "/a:/mnt/config/certs",
			wantErr: "shadow",
		},
		{
			name:    "duplicate target",
			input:   "/a:/x,/b:/x/",
			wantErr: "more than once",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseJobBindMounts(tt.input)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseJobBindMounts() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseJobBindMounts() unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parseJobBindMounts() returned %d mounts, want %d: %+v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("mount %d = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}
