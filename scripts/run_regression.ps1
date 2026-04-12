$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot\..

python -m pytest tests/test_api_server.py tests/test_simulation_events.py -q
