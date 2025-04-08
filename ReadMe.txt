一、Python环境：
1、检查预装的Python版本：python3 --version
2、安装和更新Python：sudo apt-get update   或者  sudo apt-get install python3.11.2
3、查看python路径：which python

二、程序传输：U盘/WinSCP
1、放到：/home/mrg/Documents
2、配置解释器
    使用ThonnyIDE，点击Run/运行，选中Select interpreter/配置解释器
3、配置程序运行库，打开终端输入依次指令:
	修改可以安装库的环境：
	sudo mv /usr/lib/python3.11/EXTERNALLY-MANAGED /usr/lib/python3.11/EXTERNALLY-MANAGED.bk
	
	配置镜像
	Unix:
	export PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
	source ~/.bashrc
	Windows:
    pip install --upgrade pip
    pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
	程序运行需要用到的库
	sudo pip install pandas==2.2.3
	sudo pip install ua==0.1.5
	sudo pip install asyncua==1.1.5
	sudo pip install asyncio==3.4.3
	sudo pip install bigtree==0.14.3
	sudo pip install aiohttp==3.11.11
	sudo pip install paho-mqtt==2.1.0
	sudo pip install python-snap7==2.32.3
	sudo pip install requests==1.4.1

 注意：使用 sudo pip3 install packagename代表进行全局安装，安装后全局可用。pip3 install packagename代表仅该用户的安装，安装后仅该用户可用。所以如果你当时装库时使用后者，那么运行时就要指定用户，否则会报错找不到库。
如果在运行中找不到库，解决方法有两种：
1、使用sudo pip3 install packagename再重装一遍；
2、在运行命令中指定用户


三、配置开机自启(新建/修改)
终端依次输入：
1、创建文件：sudo nano /etc/systemd/system/mrg_IOService.service
2、配置文件内容：
[Unit]
Description=MRG IOService 
After=network.target

[Service]
User=mrg  # 指定用户运行
WorkingDirectory=/home/Documents/driver_io
ExecStart=/usr/bin/ main.py 
Restart=always 
RestartSec=5 

[Install]
WantedBy=multi-user.target

3、ctrl+O：写入   ctrl+X：离开

4、相关指令：
sudo systemctl start mrg_IOService.service	启动服务
sudo systemctl stop mrg_IOService.service	停止服务
sudo systemctl restart mrg_IOService.service	重启服务
sudo systemctl status mrg_IOService.service	查看状态和日志

sudo systemctl enable mrg_IOService.service  设置开机自启
sudo systemctl disable mrg_IOService.service  关闭开机自启动
sudo systemctl daemon-reload    添加或修改配置文件后，需要重新加载

journalctl -u mrg_IOService -e   查看服务的输出
sudo journalctl -u mrg_IOService -f --no-pager   查看完整日志（按 Ctrl+C 退出）
sudo journalctl -u mrg_IOService.service -f  # 实时查看日志（等同于控制台输出）


四、Pycharm 安装（如果需要）
1、官网下载Linux版本pycharm
2、配置Java环境：sudo apt install default-jre -y
3、直接在文件夹/opt/pycharm-community-2020.3/bin中直接运行pycharm.sh文件

五、依赖包本地移植
1、生成依赖清单：pip freeze > requirements.txt
2、下载所有依赖的 `.whl` 或源码包：pip download -r requirements.txt -d ./packages
3、压缩打包：将包含`requirements.txt` 和 `packages` 程序打包，准备迁移
4、将打包的 `requirements.txt` 和 `packages` 目录复制到目标机器
5、使用本地文件安装：pip install --no-index --find-links=./packages -r requirements.txt
 --no-index：禁止从 PyPI 下载。
 --find-links=./packages：从本地目录查找依赖。

 六、远程桌面Raspbian（VNC）
1、首先 ssh 连接登录树莓派，启动自带的配置程序：sudo raspi-config
2、在弹出的配置窗口选择：Interface Options 回车  >>  选择VNC 回车 >> 选择 是 回车
3、raspbian的账户密码默认都是：mrg、mrg123456

七、Windows和Linux之间互传文件，使用WinSCP
1、首先在windows上安装WinSCP软件：https://winscp.net/download/WinSCP-6.5-Setup.exe/download
2、在Linux系统中安装ssh服务：sudo apt install openssh-server
3、将两台电脑链接到同一局域网
4、打开WinSCP，新建会话

八、忘记EMQX Dashboard密码：
C:\emqx\bin\emqx ctl admins passwd admin mrg123456



