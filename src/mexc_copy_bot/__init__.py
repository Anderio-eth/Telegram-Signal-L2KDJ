"""MEXC copy-trading bot.

Mirrors the trading actions of one MEXC futures account (Master) onto several others
(Followers), controlled from Telegram.

Runs as its own process alongside telegram_signal_k2 — see README. Keeping them separate
matters: this one places real orders on real money, and a crash in the signal bot must not
be able to leave followers half-copied.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
