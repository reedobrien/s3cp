package main

import (
	"testing"

	"github.com/reedobrien/checkers"
)

func TestRemovemePrivate(t *testing.T) {
	checkers.Equals(t, removeMe(), true)
}
