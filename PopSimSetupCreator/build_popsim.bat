call mamba env create --prefix "%~dp0\popsim" --file popsim_setup.yaml --no-default-packages
call mamba activate "%~dp0\popsim"
call mamba install conda-pack -yv -c conda-forge --override-channels
call mamba pack -p "%~dp0\popsim" -o popsim.zip
call mamba deactivate
call rmdir /s /q "%~dp0\popsim"
rem call 7za.exe x popsim.zip -o"%~dp0\..\software\popsim" -y