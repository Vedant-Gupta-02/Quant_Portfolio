Quant_Portfolio
|--Database
    |--Companies and their data (financial ratios, returns etc.(list of filters)) yearwise
|--Create formulae of a dictionary format (keys as filters and values as their (thresholds, holds for last n periods, out of total m periods)) 
|--Portfolio Creation (using Backtester)
    |--Filter out companies which satisfy formulae thresholds for (timeperiods == rebalancing frequency)
        |--Create Portfolio of these companies and find out historical returns, downside standard deviation etc. for timeperiods
        |--Plot graphs of various metrics to analyse past performance of said Portfolio
        |--Use Fundamental Backtester to analyse metrics such as sector allotment, Avg. PE etc.
    |--Select the top performing rules for each risk bucket and club them together
        |--Arrange the filtered companies according to performance and select top k companies for Portfolio
        |--Rebalance as required

|--Backtester
    |--Take input as formula and filter companies which satisfy formula thresholds for (timeperiods == rebalancing frequency)
    |--Get list of companies for each timeperiod
        |--Return Backtester 
            |--Using list of companies for each timeperiod, calculate metrics like avg. return, downside standard deviation, sharpe etc.
            |--Create graphs for the same
        |--Fundamental Backtester
            |--Using list of companies for each timeperiod, analyse fundamental factors like sector allotment, PE ratios etc. for the overall portfolio
            |--Create graphs for the same

|--Rule Creation
    |--Create random n rules using various combination of filters and thresholds
    |--Backtest these n rules and get results
    |--Create a rule tweaker using randomization or AI techniques
        |--Select rule which has maximum chance of improvement according to mean returns vs downside standard deviation
        |--Tweak rule in a way which will give maximal chance of improvement for selected rule
        |--Reinforce
        |--Can use genetic techniques, Bayes optimization (to find optimal thresholds), RL
    |--Train models to find market regimes ad dynamically adjust thresholds based on this

|--Data
    |--Database
        |--3 layers:
            |--Company Info (static) → sector, listing date, etc.: companies(id, name, sector)
            |--Financials (yearly ratios): financials(company_id, year, ratio_name, value)
            |--Returns (daily/monthly prices, dividends): prices(company_id, date, close_price, dividend)
    |--Predictive Models for Returns
        |--Train ML models to predict future values of company data and use that in rule creation
    |--Feature Engineering
        |--Use PCA/Autoencoders to compress multiple ratios into a single metric
        |--Cluster companies by fundamental profiles
        |--Tree based feature importance to find out historically which ratios mattered for most returns

|--Portfolio Weightage
    |--Markowitz Optimizer with ML Forecasts
        |--Predict expected returns (ML) + estimate covariances.
        |--Feed into modern portfolio theory to get optimal weights.
    |--Reinforcement Learning
        |--State = portfolio holdings + market data.
        |--Action = rebalance decisions.
        |--Reward = risk-adjusted return.
    |--Deep Hedging (Neural Nets)
        |--Train neural nets to dynamically rebalance for risk control
    |--Graph correlation based asset reallocation
    |--Use ML techniques to find out whihc timeperiod rebalancing gives the most profits

|--Explainability
    |--Use shapely values for ML models to find out which ratio mattered most for which stock pick
