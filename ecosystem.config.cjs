module.exports = {
  apps: [
    {
      name: "telegram-signal-k2",
      script: ".venv/bin/python",
      args: "-m telegram_signal_k2",
      cwd: __dirname,
      interpreter: "none",
      autorestart: true,
      restart_delay: 5000,
      max_restarts: 50,
      time: true,
      env: {
        PYTHONUNBUFFERED: "1"
      }
    }
  ]
};

