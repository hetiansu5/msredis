package msredis

import (
	"context"
	"github.com/gogf/gf/database/gredis"
	"testing"
)

func TestNew_SameSlave(t *testing.T) {
	g := New("cache", "cache_slave1")
	_, err := g.DoVar("SET", "test02", "23")
	if err != nil {
		t.Error(err)
	}
	v1, err1 := g.DoVar("GET", "test02")
	if err1 != nil {
		t.Error(err1)
	}
	if v1.String() != "23" {
		t.Error("no equal value")
	}
}

func TestNew_DiffSlave(t *testing.T) {
	g := New("cache", "cache_slave2")
	_, err := g.DoVar("SET", "test03", "wa")
	if err != nil {
		t.Error(err)
	}
	v1, err1 := g.DoVar("GET", "test03")
	if err1 != nil {
		t.Error(err1)
	}
	if v1.String() == "wa" {
		t.Error("no use slave")
	}
}

func TestGroup_NoSlave(t *testing.T) {
	g := Group()
	_, err := g.DoVar("SET", "test04", "haa")
	if err != nil {
		t.Error(err)
	}
	v1, err1 := g.DoVar("GET", "test04")
	if err1 != nil {
		t.Error(err1)
	}
	if v1.String() != "haa" {
		t.Error("no equal value")
	}

	if _, ok := redisGroupMap[gredis.DefaultGroupName]; !ok {
		t.Error("redisGroupMap cache not exist")
	}

	g1 := Group(gredis.DefaultGroupName)
	if g1 != g {
		t.Error("no same group")
	}
}

func TestCtx(t *testing.T) {
	g := New("default")
	ctx := context.TODO()
	v, err := g.Ctx(ctx).DoVar("SET", "test02", "23")
	if err != nil {
		t.Error(err)
	}
	if v.String() != "OK" {
		t.Error("not return ok")
	}
}
