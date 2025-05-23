package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
)

var (
	moviesServiceURL = os.Getenv("MOVIES_SERVICE_URL")
	monolithURL      = os.Getenv("MONOLITH_URL")
	migrationEnabled = os.Getenv("GRADUAL_MIGRATION") == "true"
	migrationPercent = getPercentFromEnv("MOVIES_MIGRATION_PERCENT", 0)
	serverPort       = os.Getenv("PORT")
)

func main() {
	http.HandleFunc("/", proxyHandler)
	fmt.Println("STARTING ON: ", ":"+serverPort)
	http.ListenAndServe(":"+serverPort, nil)
}

func proxyHandler(rw http.ResponseWriter, r *http.Request) {
	target := monolithURL
	if shouldUseMicroservice(r) {
		target = moviesServiceURL
	}

	remote, err := url.Parse(target)
	if err != nil {
		http.Error(rw, "Bad target URL", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(remote)
	proxy.ServeHTTP(rw, r)
}

func shouldUseMicroservice(r *http.Request) bool {
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		userID = r.RemoteAddr
	}

	percent := hashToPercent(userID)
	return percent < migrationPercent
}

func hashToPercent(userID string) int {
	hash := md5.Sum([]byte(userID))
	return int(binary.BigEndian.Uint16(hash[:2]) % 100)
}

func getPercentFromEnv(envName string, fallback int) int {
	envValue := os.Getenv(envName)
	if i, err := strconv.Atoi(envValue); err == nil {
		return i
	}

	return fallback
}
