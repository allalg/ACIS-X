# ──────────────────────────────────────────────────────────────────────────────
# ACIS-X Frontend (React + Vite)
# ──────────────────────────────────────────────────────────────────────────────
# Multi-stage build: Node for building, nginx for serving.
#
# Build: docker build -t acis-frontend .
# Run:   docker run -p 3000:80 acis-frontend
# ──────────────────────────────────────────────────────────────────────────────

# Stage 1: Build the Vite app
FROM node:20-alpine AS build

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci
COPY . .

# Build-time environment variable for the API URL
ARG VITE_API_BASE_URL=http://localhost:8000
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL

RUN npm run build

# Stage 2: Serve with nginx
FROM nginx:alpine

# Copy built assets
COPY --from=build /app/dist /usr/share/nginx/html

# SPA routing: redirect all 404s to index.html
RUN echo 'server { \
    listen 80; \
    server_name _; \
    root /usr/share/nginx/html; \
    index index.html; \
    location / { \
        try_files $uri $uri/ /index.html; \
    } \
}' > /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
