package config

import (
	"strings"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	c := Default()
	c.Source.Hosts = []string{"a"}
	c.Target.Hosts = []string{"a"}
	c.Target.ReplicasPerShard = -1
	c.Finalize()
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "и в source, и в target") || !strings.Contains(err.Error(), "replicas_per_shard") {
		t.Fatal(err)
	}
}
