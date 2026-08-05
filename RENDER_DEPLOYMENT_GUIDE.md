# 🚀 Render 100% Free 1-Click Deployment Guide

Follow these 3 simple steps to deploy ACIS-X live online for **100% free** with **zero credit card required**.

---

### Step 1: Push Your Repository to GitHub
Make sure all your latest code (including `render.yaml` and `Dockerfile.prod`) is pushed to your GitHub repository:
```bash
git add .
git commit -m "Add production Render blueprint and unified Dockerfile"
git push origin main
```

---

### Step 2: Deploy Blueprint on Render (3 Clicks)
1. Go to **[dashboard.render.com](https://dashboard.render.com/)** and log in with your GitHub account.
2. Click the **New +** button in the top right header $\rightarrow$ Select **Blueprint**.
3. Connect your **ACIS-X** repository.
4. Render will read `render.yaml` and show 3 services:
   * **`acis-frontend`** (Static Site - Free SSL, Global CDN)
   * **`acis-backend`** (Web Service - Unified FastAPI + 15 Autonomous Agents)
   * **`acis-kafka`** (Private Kafka Service - Internal event bus)
5. Click **Apply**.

---

### Step 3: Access Your Live App!
Render will automatically build and deploy all services:
* **Live Website URL**: Render will generate a free HTTPS URL for your frontend (e.g. `https://acis-frontend.onrender.com`).
* **Live API & SSE Stream**: `https://acis-backend.onrender.com`.

Everything will run 24/7 online in the cloud with zero local machine requirements!
