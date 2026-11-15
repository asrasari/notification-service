// Package api holds the OpenAPI specification, the API's contract (D-016).
// It is written by hand; contract tests check every response against it.
package api

import _ "embed"

// Spec is api/openapi.yaml, served at GET /openapi.yaml.
//
//go:embed openapi.yaml
var Spec []byte
