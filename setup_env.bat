rem ### CODE OWNERS: Shea Parkes, Umang GUpta
rem
rem ### OBJECTIVE:
rem   Configure environment for use so it can be utilized by multiple systems
rem
rem ### DEVELOPER NOTES:
rem   <none>

rem ### LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Setting up nyhealth-cms-quality-measures-detail env
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Running from %~f0

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Calling last promoted pipeline_components_env.bat
call "S:\PRM\Pipeline_Components_Env\pipeline_components_env.bat"

echo.
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Redirecting NYHEALTH_CMS_QUALITY_MEASURES_DETAIL_HOME to local copy
SET NYHEALTH_CMS_QUALITY_MEASURES_DETAIL_HOME=%~dp0%
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: NYHEALTH_CMS_QUALITY_MEASURES_DETAIL_HOME is now %NYHEALTH_CMS_QUALITY_MEASURES_DETAIL_HOME%

echo.
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Prepending local copy of python library to PYTHONPATH
set PYTHONPATH=%NYHEALTH_CMS_QUALITY_MEASURES_DETAIL_HOME%python;%PYTHONPATH%
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: PYTHONPATH is now %PYTHONPATH%

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Finished setting up nyhealth-cms-quality-measures-detail env
