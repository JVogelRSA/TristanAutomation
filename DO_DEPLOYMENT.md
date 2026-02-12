# Deploying to DigitalOcean

This guide explains how to move the bots from GitHub Actions to your own DigitalOcean Droplet.

## 1. Create a Droplet
1.  Log in to [DigitalOcean](https://cloud.digitalocean.com/).
2.  Click **Create** -> **Droplets**.
3.  **Region**: Choose data center closest to you (e.g., London or Frankfurt if no SA option).
4.  **Image**: Choose **Ubuntu 22.04 (LTS)**.
5.  **Size**: **Basic Plan**, Regular SSD, **$6/month** (1GB RAM) is sufficient.
6.  **Authentication**: Choose **SSH Key** (Recommended) or Password.
7.  **Hostname**: Name it `tristan-automation`.
8.  Click **Create Droplet**.

## 2. Connect to the Server
Once created, copy the IP address. Open your terminal:
```bash
ssh root@YOUR_DROPLET_IP
# If asked, type 'yes' and enter your password.
```

## 3. Run the Setup Script
I have included a script to automate the installation.
Run these commands on the server:

```bash
# 1. Download the repo (Public)
git clone https://github.com/JVogelRSA/TristanAutomation.git

# 2. Go into the folder
cd TristanAutomation

# 3. Make the script executable
chmod +x setup_server.sh

# 4. Run it
./setup_server.sh
```

## 4. Add Your Secrets (.env)
The server needs your passwords (API keys).
1.  Type: `nano .env`
2.  Paste the contents of your local `.env` file into this screen.
3.  Press **Ctrl+X**, then **Y**, then **Enter** to save.

## 5. Verify
Test the bots manually to make sure they work:
```bash
source venv/bin/activate
python test_connections.py
```

## 6. Automatic Schedule (Cron)
The setup script already configured **Cron** (Linux's scheduler) to run the bots every Monday morning.
-   Inventory Bot: 10:00 AM UTC
-   Spend Bot: 10:05 AM UTC
-   Sales Bot: 10:10 AM UTC

You can check the logs anytime:
```bash
cat inventory.log
```
