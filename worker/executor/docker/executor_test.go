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
			input: `[{"source":"/opt/certs","target":"/etc/ssl/custom-certs"}]`,
			want:  []mount.Mount{{Type: mount.TypeBind, Source: "/opt/certs", Target: "/etc/ssl/custom-certs", ReadOnly: true}},
		},
		{
			name:  "target is cleaned",
			input: `[{"source":"/opt/certs","target":"/etc//ssl/./certs/"}]`,
			want:  []mount.Mount{{Type: mount.TypeBind, Source: "/opt/certs", Target: "/etc/ssl/certs", ReadOnly: true}},
		},
		{
			name:  "multiple mounts",
			input: `[{"source":"/a","target":"/x"},{"source":"/b","target":"/y"}]`,
			want: []mount.Mount{
				{Type: mount.TypeBind, Source: "/a", Target: "/x", ReadOnly: true},
				{Type: mount.TypeBind, Source: "/b", Target: "/y", ReadOnly: true},
			},
		},
		{
			name:  "path next to the config dir",
			input: `[{"source":"/a","target":"/mnt/configs"}]`,
			want:  []mount.Mount{{Type: mount.TypeBind, Source: "/a", Target: "/mnt/configs", ReadOnly: true}},
		},
		{
			name:    "invalid json",
			input:   `{`,
			wantErr: "unmarshal job bind mounts",
		},
		{
			name:    "not a list",
			input:   `{"source":"/a","target":"/x"}`,
			wantErr: "unmarshal job bind mounts",
		},
		{
			name:    "unknown field",
			input:   `[{"source":"/a","target":"/x","read_only":true}]`,
			wantErr: "unmarshal job bind mounts",
		},
		{
			name:    "relative source",
			input:   `[{"source":"certs","target":"/x"}]`,
			wantErr: "absolute host path",
		},
		{
			name:    "missing source",
			input:   `[{"target":"/x"}]`,
			wantErr: "absolute host path",
		},
		{
			name:    "relative target",
			input:   `[{"source":"/a","target":"x"}]`,
			wantErr: "absolute container path",
		},
		{
			name:    "missing target",
			input:   `[{"source":"/a"}]`,
			wantErr: "absolute container path",
		},
		{
			name:    "target is the container root",
			input:   `[{"source":"/a","target":"/"}]`,
			wantErr: "absolute container path",
		},
		{
			name:    "target is the config dir",
			input:   `[{"source":"/a","target":"/mnt/config"}]`,
			wantErr: "shadow",
		},
		{
			name:    "target inside the config dir",
			input:   `[{"source":"/a","target":"/mnt/config/certs"}]`,
			wantErr: "shadow",
		},
		{
			name:    "duplicate target",
			input:   `[{"source":"/a","target":"/x"},{"source":"/b","target":"/x/"}]`,
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
