//go:build debug

package stream

// The retained debug variant adds no API or runtime behavior. Historical
// commented-out coordinator introspection was not a supported entrypoint;
// ownership-sensitive inspection should use explicit snapshots instead.
