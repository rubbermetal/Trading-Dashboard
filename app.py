from flask import Flask, render_template
from routes.screener import screener_bp
from routes.portfolio import portfolio_bp
from routes.trading import trading_bp
from routes.market_data import market_data_bp
from routes.bot_manager import bot_manager_bp

app = Flask(__name__)

# Register Modules
app.register_blueprint(screener_bp)
app.register_blueprint(portfolio_bp)
app.register_blueprint(trading_bp)
app.register_blueprint(market_data_bp)
app.register_blueprint(bot_manager_bp)

@app.route('/')
def home(): 
    return render_template('index.html')

if __name__ == '__main__': 
    app.run(host='0.0.0.0', port=5000)
