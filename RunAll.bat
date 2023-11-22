::~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:: This batch file runs the entire process of Population synthesis from downloading data from Census,
:: building corsswalks, building controls and running PopulationSim. 
:: User should specify the following
::      - All required inputs
::      - Local directories
:: 		- Local Anaconda installation directory
::		- Assumes SetUpPopulationSim.bat has been run and Conda environment "popsim" exists
:: Khademul Haque, khademul.haque@rsginc.com, 04/03/2019
:: Edited by Hannah Carson, hannah.carson@rsginc.com, 04/25/2019

:: TO DO
:: 1. make sure python call is from the same anaconda directory as in the PopulationSim setup folder
::~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@ECHO OFF
ECHO %startTime%%Time%

:: FLAGS - YES/NO
SET RUN_CROSSWALK=YES
SET RUN_CONTROLS=YES
SET RUN_POPSIM_HH=YES
SET RUN_POPSIM_GQ=YES
SET RUN_VALIDATION=YES
SET RUN_DAYSIMFORMAT=YES

:: Set Inputs and Directories
SET WORKING_DIR=%~dp0
SET DATA_DIR=%WORKING_DIR%Data
SET MAZ_DIR=%DATA_DIR%\MAZ
SET TAZ_DIR=%DATA_DIR%\TAZ
SET CT_DIR=%DATA_DIR%\CensusTract
SET BG_DIR=%DATA_DIR%\BlockGroup
SET PUMA_DIR=%DATA_DIR%\PUMS\Geography
SET CNTY_DIR=%DATA_DIR%\County
SET XWALK_DIR=%WORKING_DIR%Setup\Data
SET POPSIMDIR=%WORKING_DIR%Setup
SET VALID_DIR=%WORKING_DIR%Setup\validation
:: Anaconda installation directory
SET ANACONDA_DIR=%WORKING_DIR%Setup\software\popsim
SET ACTIVATE_POPSIM=%ANACONDA_DIR%\Scripts\activate.bat
SET R_SCRIPT=%WORKING_DIR%Setup\software\R\R-3.4.4\bin\x64\Rscript
:: ---------------------------------------------------------------------
:: Create Parameters file for R scripts
SET PARAMETERS_FILE=%DATA_DIR%\parameters.csv

ECHO Key,Value > %PARAMETERS_FILE%
ECHO WORKING_DIR,%WORKING_DIR% >> %PARAMETERS_FILE%
ECHO MAZ_DIR,%MAZ_DIR% >> %PARAMETERS_FILE%
ECHO TAZ_DIR,%TAZ_DIR% >> %PARAMETERS_FILE%
ECHO CT_DIR,%CT_DIR% >> %PARAMETERS_FILE%
ECHO BG_DIR,%BG_DIR% >> %PARAMETERS_FILE%
ECHO PUMA_DIR,%PUMA_DIR% >> %PARAMETERS_FILE%
ECHO CNTY_DIR,%CNTY_DIR% >> %PARAMETERS_FILE%
ECHO XWALK_DIR,%XWALK_DIR% >> %PARAMETERS_FILE%
ECHO POPSIMDIR,%POPSIMDIR% >> %PARAMETERS_FILE%
ECHO VALID_DIR,%VALID_DIR% >> %PARAMETERS_FILE%

:: Create geographic crosswalk
IF %RUN_CROSSWALK%==YES (
	ECHO %startTime%%Time%: Running script to create crosswalk...
	call %ACTIVATE_POPSIM%
	call python %WORKING_DIR%Scripts\createGeogXWalks.py %PARAMETERS_FILE% > Setup\logs\createGeoXWalks.log 2>&1
)
	
:: Download census data and create Controls
IF %RUN_CONTROLS%==YES (
	ECHO %startTime%%Time%: Create controls...
	call %ACTIVATE_POPSIM%
	call python %WORKING_DIR%scripts\downloadCensusData.py %PARAMETERS_FILE% > Setup\logs\downloadCensus.log 2>&1
	call python %WORKING_DIR%scripts\buildControls.py %PARAMETERS_FILE% > Setup\logs\buildControls.log 2>&1
)

:: Run PopulationSim for HH population
IF %RUN_POPSIM_HH%==YES (
	ECHO %startTime%%Time%: Running PopulationSim...
	CALL %POPSIMDIR%\RunPopulationSimHH.bat > Setup\Logs\populationSimHH.log 2>&1
	cd..
)

:: Run PopulationSim for GQ population
IF %RUN_POPSIM_GQ%==YES (
	ECHO %startTime%%Time%: Running PopulationSim for group quarters...
	CALL %POPSIMDIR%\RunPopulationSimGQ.bat > Setup\logs\populationSimGQ.log 2>&1
	cd..
)

:: Merge HH and GQ population
IF %RUN_POPSIM_HH%==YES (
	ECHO merging Group Quarters
	call %ACTIVATE_POPSIM%
	call python %WORKING_DIR%scripts\mergeHHandGQ.py %PARAMETERS_FILE% > Setup\logs\mergePopulation.log 2>&1
) ELSE (
	IF %RUN_POPSIM_GQ%==YES (
		ECHO merging Group Quarters
	    call %ACTIVATE_POPSIM%
	    call python %WORKING_DIR%scripts\mergeHHandGQ.py %PARAMETERS_FILE% > Setup\logs\mergePopulation.log 2>&1
	)
)

:: Run Validation
IF %RUN_VALIDATION%==YES (
	ECHO %startTime%%Time%: Running R script to create Popsim validation plots...
	%R_SCRIPT% %WORKING_DIR%\scripts\validatePopulationSim.R %PARAMETERS_FILE% > Setup\logs\validatePopulationSim.log 2>&1
)

:: Run converting PopulationSim outputs to DaySim format
IF %RUN_DAYSIMFORMAT%==YES (
	ECHO %startTime%%Time%: Running R script to create DaySim Inputs...
	%R_SCRIPT% %WORKING_DIR%\scripts\popsimToDaysim.R %PARAMETERS_FILE% > Setup\logs\popsimToDaysim.log 2>&1
	
	
	ECHO %startTime%%Time%: Running R script to create DaySim Inputs for GQ...
	%R_SCRIPT% %WORKING_DIR%\scripts\popsimToDaysim_GQ.R %PARAMETERS_FILE% > Setup\logs\popsimToDaysim_GQ.log 2>&1
	
	ECHO %startTime%%Time%: Running Python script to combine DaySim Inputs for GQ...
	call %ACTIVATE_POPSIM%
	call python %WORKING_DIR%scripts\popsimToDaysimMerge.py %PARAMETERS_FILE% > Setup\logs\mergeDaySimPopulation.log 2>&1
	
)

ECHO Success!