package web

import "embed"

// Static contains the workbench browser application.
//
//go:embed static/*
var Static embed.FS
