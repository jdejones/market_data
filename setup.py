from setuptools import setup, find_packages

setup(
    name="market_data_processing",
    version="0.1.0",
    packages=find_packages(),    # will pick up the `market_data` package
    install_requires=[
        "backtesting",
        # e.g. "pandas>=1.2", "numpy"
    ],
    entry_points={
        "console_scripts": [
            # optional: "run-study = market_data.daily_after_close_study:main"
        ]
    }
)
