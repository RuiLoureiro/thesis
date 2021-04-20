from setuptools import setup, find_packages

install_reqs = [
    "pyyaml",
    "numpy",
    "pandas",
    "tqdm",
    "loguru",
    "rich",
    "polyline",
    # "graph_tool",
    "requests",
    "tornado",
    "nest_asyncio",
]

setup(
    name="thesis",
    version="1.0.0",
    author="Rui Loureiro",
    author_email="rui.loureiro@tecnico.ulsiboa.pt",
    description="My thesis code",
    packages=find_packages(),
    install_requires=install_reqs,
)
