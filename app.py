import os
import stat
from flask import Flask, render_template


def _check_permissions():
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        mode = stat.S_IMODE(os.stat(env_path).st_mode)
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            print(f"WARNING: .env is readable by group/others (mode {oct(mode)}). Run: chmod 600 .env")


_check_permissions()
from routes.screener import screener_bp
from routes.portfolio import portfolio_bp
from routes.trading import trading_bp
from routes.market_data import market_data_bp
from routes.bot_manager import bot_manager_bp
from routes.scanner import scanner_bp
from routes.equity import equity_bp
from routes.backtest import backtest_bp

app = Flask(__name__)

# Register Modules
app.register_blueprint(screener_bp)
app.register_blueprint(portfolio_bp)
app.register_blueprint(trading_bp)
app.register_blueprint(market_data_bp)
app.register_blueprint(bot_manager_bp)
app.register_blueprint(scanner_bp)
app.register_blueprint(equity_bp)
app.register_blueprint(backtest_bp)

@app.route('/mobile')
def mobile():
    return render_template('mobile.html')

@app.route('/')
def home():
    response = app.make_response(render_template('index.html'))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

if __name__ == '__main__':
    from routes.trading import start_manual_evaluator
    start_manual_evaluator()
    app.run(host='0.0.0.0', port=5000)
