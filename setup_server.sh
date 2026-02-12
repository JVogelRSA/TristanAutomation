#!/bin/bash
# setup_server.sh
# Run this on your DigitalOcean Droplet to set up the environment.

set -e

echo "🚀 Starting Server Setup..."

# 1. Update System
echo "📦 Updating system packages..."
sudo apt-get update && sudo apt-get upgrade -y

# 2. Install Python & Utilities
echo "🐍 Installing Python 3 and tools..."
sudo apt-get install -y python3 python3-pip python3-venv git cron

# 3. Clone Repository (You will need to use your GitHub Personal Access Token or SSH Key)
# Check if repo exists
if [ -d "TristanAutomation" ]; then
    echo "📂 Repo already exists. Pulling latest..."
    cd TristanAutomation
    git pull
else
    echo "📂 Cloning repository..."
    # User will need to run this manually or we use a public repo url if public
    # Assuming public for now based on 'gh repo create --public' command earlier
    git clone https://github.com/JVogelRSA/TristanAutomation.git
    cd TristanAutomation
fi

# 4. Set up Virtual Environment
echo "🛠️ Creating Virtual Environment..."
python3 -m venv venv
source venv/bin/activate

# 5. Install Dependencies
echo "📥 Installing Python dependencies..."
pip install -r requirements.txt

# 6. Setup Cron Jobs (Scheduling)
echo "⏰ Configuring Cron Jobs..."

# Backup current crontab
crontab -l > mycron 2>/dev/null || true

# Add Inventory Bot (Every Monday at 10:00 AM UTC)
# 0 10 * * 1
grep -q "inventory_bot.py" mycron || echo "0 10 * * 1 cd ~/TristanAutomation && source venv/bin/activate && python inventory_bot.py >> inventory.log 2>&1" >> mycron

# Add Spend Bot (Every Monday at 10:05 AM UTC)
# 5 10 * * 1
grep -q "spend_bot.py" mycron || echo "5 10 * * 1 cd ~/TristanAutomation && source venv/bin/activate && python spend_bot.py >> spend.log 2>&1" >> mycron

# Add Sales Bot (Every Monday at 10:10 AM UTC)
# 10 10 * * 1
grep -q "sales_bot.py" mycron || echo "10 10 * * 1 cd ~/TristanAutomation && source venv/bin/activate && python sales_bot.py >> sales.log 2>&1" >> mycron

# Install new cron file
crontab mycron
rm mycron

echo "✅ Server Setup Complete!"
echo "⚠️  IMPORTANT: You must create the .env file with your secrets!"
echo "   Run: nano .env"
