# ComfyUI-Distributed-Setup
# ComfyUI Distributed GPU Farm

A distributed ComfyUI rendering platform that allows a single ComfyUI interface to utilize multiple GPUs across one or more machines.

The project provides:

* Centralized ComfyUI Master UI
* RabbitMQ job queue
* Distributed worker scheduling
* Multi-GPU load balancing
* Shared storage support (SMB/NFS)
* Docker deployment
* Real-time job tracking
* Horizontal scaling across multiple hosts

---

# Architecture

## Single Host Deployment

```text
┌───────────────────────────────────────────┐
│                 Main Host                 │
├───────────────────────────────────────────┤
│                                           │
│  ComfyUI Master GUI                       │
│          │                                │
│          ▼                                │
│      Comfy API                            │
│          │                                │
│          ▼                                │
│      RabbitMQ                             │
│          │                                │
│          ▼                                │
│     Dispatcher                            │
│          │                                │
│   ┌──────┴──────┐                         │
│   ▼             ▼                         │
│ Worker 3080   Worker 2070                 │
│                                           │
└───────────────────────────────────────────┘
```

---

## Multi-Host Deployment

```text
                              USER
                                │
                                ▼

                    https://ai-master.domain.com

                                │
                                ▼

┌─────────────────────────────────────────────────────┐
│                    AI MASTER                        │
│                 192.168.1.10                        │
├─────────────────────────────────────────────────────┤
│                                                     │
│ ComfyUI Master GUI                                  │
│ Comfy API                                           │
│ RabbitMQ                                            │
│ Dispatcher                                          │
│                                                     │
└───────────────┬──────────────────────┬──────────────┘
                │                      │
                │                      │
                ▼                      ▼

┌──────────────────────┐   ┌──────────────────────┐
│ GPU NODE 01          │   │ GPU NODE 02          │
│ 192.168.1.21         │   │ 192.168.1.22         │
│ RTX 3080             │   │ RTX 2070             │
│ ComfyUI Worker       │   │ ComfyUI Worker       │
└──────────┬───────────┘   └──────────┬───────────┘
           │                          │
           └─────────────┬────────────┘
                         │
                         ▼

┌──────────────────────────────────────────┐
│          Shared SMB / NFS Storage         │
│            192.168.1.50                   │
│                                            │
│ /input                                     │
│ /output/worker01                           │
│ /output/worker02                           │
│ /models                                    │
└──────────────────────────────────────────┘
```

---

# Requirements

## Master Server

Required software:

* Docker Engine
* Docker Compose
* RabbitMQ
* Comfy API
* Dispatcher
* Nginx Reverse Proxy

Hardware:

* CPU: 4+ cores
* RAM: 8GB minimum
* SSD recommended

No GPU required.

---

## Worker Servers

Required software:

* Docker Engine
* Docker Compose
* NVIDIA Drivers
* NVIDIA Container Toolkit
* ComfyUI Worker

Hardware:

* NVIDIA GPU
* CUDA-compatible driver
* SSD storage

Examples:

* RTX 2070
* RTX 3080
* RTX 4070
* RTX 4090

---

# Variables You MUST Change

The following values are examples and MUST be replaced.

---

## Master Server IP

Replace:

```yaml
192.168.1.10
```

With:

```yaml
YOUR_MASTER_IP
```

Example:

```yaml
192.168.50.100
```

---

## Worker IP Addresses

Replace:

```yaml
COMFY_WORKERS: >
  http://192.168.1.21:8188,
  http://192.168.1.22:8188
```

With your worker IPs:

```yaml
COMFY_WORKERS: >
  http://10.0.0.101:8188,
  http://10.0.0.102:8188,
  http://10.0.0.103:8188
```

---

## RabbitMQ Password

Replace:

```yaml
RABBITMQ_DEFAULT_PASS: ChangeThisPassword
```

With:

```yaml
RABBITMQ_DEFAULT_PASS: YOUR_STRONG_PASSWORD
```

---

## SMB Share Location

Replace:

```text
//192.168.1.50/comfy-smb
```

With:

```text
//YOUR_FILESERVER/comfy-smb
```

Example:

```text
//nas.company.local/comfy-smb
```

---

## SMB Mount Point

Current:

```text
/mnt/comfy-output
```

Can be changed to:

```text
/data/comfy-storage
```

If changed, update all volume mounts accordingly.

---

## Reverse Proxy URL

Replace:

```text
https://ai-master.domain.com
```

With:

```text
https://your-domain.com
```

Examples:

```text
https://comfy.company.com
https://ai.lab.local
https://render.mydomain.com
```

---

# Shared Storage Structure

Required directory structure:

```text
comfy-smb/
│
├── input/
│
├── output/
│   ├── worker01/
│   ├── worker02/
│   ├── worker03/
│
└── models/
```

---

# Model Synchronization

All workers must contain identical:

```text
models/
custom_nodes/
ComfyUI version
```

Recommended methods:

* rsync
* Syncthing
* NAS shared models
* DFS Namespace

Example:

```bash
rsync -avh worker01:/models/ worker02:/models/
```

---

# Deploying A New Worker

Install:

```bash
Docker
Docker Compose
NVIDIA Drivers
NVIDIA Container Toolkit
```

Mount SMB share:

```bash
sudo mkdir -p /mnt/comfy-output

sudo mount -t cifs \
//YOUR_FILESERVER/comfy-smb \
/mnt/comfy-output \
-o username=USER,password=PASSWORD,uid=1024,gid=1024,file_mode=0777,dir_mode=0777
```

Update dispatcher:

```yaml
COMFY_WORKERS:
  http://192.168.1.21:8188,
  http://192.168.1.22:8188,
  http://192.168.1.23:8188
```

Restart dispatcher:

```bash
docker compose restart comfy-dispatcher
```

---

# Security Recommendations

Production deployments should add:

* HTTPS
* Authentik
* Authelia
* Cloudflare Access
* VPN access
* Firewall rules
* Worker network isolation

Recommended:

```text
Internet
   │
Cloudflare
   │
Traefik / Nginx
   │
Authentik
   │
ComfyUI Master
```

---

# Current Features

* RabbitMQ Job Queue
* Multi-GPU Scheduling
* Multi-Host Support
* Shared Storage Support
* Docker Deployment
* Worker Health Checks
* Automatic Worker Selection
* Job Tracking API
* WebSocket Status Updates
* SMB Storage Support

---

# Future Improvements

* User Authentication
* Per-user Queues
* Priority Scheduling
* Automatic Worker Discovery
* Kubernetes Deployment
* GPU Capability Detection
* Metrics Dashboard
* Grafana Integration

---

# Disclaimer

This project is an experimental distributed rendering platform built on top of ComfyUI.

It is not affiliated with or endorsed by the official ComfyUI project.

Test thoroughly before deploying in production environments.
