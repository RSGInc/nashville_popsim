
::~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:: Runs PopulationSim for GQs. User should specify the following
:: 		- Local Anaconda installation directory
::		- Assumes SetUpPopulationSim.bat has been run and Conda environment "popsim" exists
:: Binny Paul, binny.mathewpaul@rsginc.com, 121517
::~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
@ECHO OFF
ECHO %startTime%%Time%
SET BATCH_DIR=%~dp0
:: setup paths to Python application, Conda script, etc.
SET CONDA_ACT=%ANACONDA_DIR%\Scripts\activate.bat

:: run populationsim
ECHO Running PopulationSim....
CALL %CONDA_ACT%
CD %BATCH_DIR%


CALL python run_populationsim.py --config configs\GQ --o output\GQ

ECHO PopulationSim run complete!!
ECHO %startTime%%Time%
