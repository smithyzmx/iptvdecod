from setuptools import setup, find_packages

setup(
    name="iptv",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "flask>=2.0.0",
        "requests>=2.25.0",
        "waitress>=3.0.0"
    ],
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "iptv=iptv.xc_server:run_service"
        ]
    }
)
