@echo off
REM 确保控制台支持UTF-8
chcp 65001 >nul

REM 显示当前工作目录（用于诊断路径问题）
echo 当前工作目录: %cd%

REM 定义程序路径
set "EXE_PATH=tools\icp\icpApi.exe"
echo 程序路径: %EXE_PATH%

REM 检查文件是否存在
if exist "%EXE_PATH%" (
    echo 发现程序文件，尝试启动...
    
    REM 使用绝对路径启动，并等待程序响应
    start "" /D "%~dp0tools\icp" "icpApi.exe"
    
    REM 检查启动是否成功
    if %errorlevel% equ 0 (
        echo ICP API服务已成功启动
    ) else (
        echo 启动失败，错误代码: %errorlevel%
        pause
    )
) else (
    echo 错误：未找到ICP API服务程序
    echo 请确认文件是否存在于以下路径：%cd%\%EXE_PATH%
    pause
)