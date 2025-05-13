一、Python环境：
1、检查预装的Python版本：python3 --version
2、安装和更新Python：sudo apt-get update   或者  sudo apt-get install python3.11.2
3、查看python路径：which python
4、点击右上角有线连接状态 > 编辑链接 > 有线连接 > 设置按钮 > IPv4设置 > Method 选择Manual/手动 > Add > 输入Ip地址 从 192.168.20.90开始 > 保存

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
	sudo pip install python-snap7
	sudo pip install requests

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
User=mrg
WorkingDirectory=/home/mrg/Documents/driver_io
ExecStart=/usr/bin/python main.py
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


Windows系统配置开机自启：
a、 Win + R，输入 shell:startup，回车打开当前用户的启动文件夹
b、将快捷方式拖入启动文件夹中即可

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
1、配置SSH服务：首先 ssh 连接登录树莓派，启动自带的配置程序：sudo raspi-config
2、在弹出的配置窗口选择：Interface Options 回车  >>  选择VNC 回车 >> 选择 是 回车
3、raspbian的账户密码默认都是：mrg、mrg123456，ip从192.168.20.91开始
4、系统时间同步：输入sudo dpkg-reconfigure tzdata 回车，进入下面截图，选择亚洲Asia，回车；选择需要设置的时区，选择上海，回车确定；
5、Windows系统配置SSH服务：
    a、搜索“可选功能”
    b、点击查看功能
    c、找到OpenSSH服务器，勾选>下一步>添加，等待添加成功
    d、返回到“服务”应用（使用services.msc命令）
    e、找到“OpenSSH SSH Server”服务
    f、右键点击服务，选择“启动”来启动SSH服务
    g、右键点击服务，选择“属性”，在“启动类型”下拉菜单中选择“自动”，然后点击“确定”。
    h、为了确保SSH服务正常工作，可能需要对其进行一些基本配置：设置防火墙规则以允许SSH连接。在“控制面板”中的“Windows Defender 防火墙”设置中，允许端口22（SSH默认端口）的入站连接。

七、Windows和Linux之间互传文件，使用WinSCP/Drv_tool(远程模式)
1、首先在windows上安装WinSCP软件：https://winscp.net/download/WinSCP-6.5-Setup.exe/download
2、在Linux系统中安装ssh服务：sudo apt install openssh-server
3、将两台电脑链接到同一局域网
4、打开WinSCP，新建会话 / Drv_tool(远程模式) 使用SFTP添加配置
5、Drv_tool(远程模式) 使用SMB/CIFS访问Windows文件夹，首先把该文件夹设置高级共享，比如：
      a、打开 文件资源管理器，右键点击 D:\pro\pro\py_pro\company_pro\driver_io 文件夹，选择 “属性” → “共享” → “高级共享”。
      b、勾选 “共享此文件夹”，在 “共享名” 一栏填入 driver_io，点击 “确定”。


八、Mqtt broker配置
1、mqtt最大报文配置：2mb
2、mqtt最大qos：2
3、忘记EMQX Dashboard密码：C:\emqx\bin\emqx ctl admins passwd admin mrg123456

九、驱动黑盒driver config.json配置
1、驱动层的Basic中的blockId为100、index从101开始、category是Driver、name是MRG_RP

十、时间同步问题：
1、Windows系统搭建NTP服务器：
    win+R:gpedit.msc，然后在本地组策略编辑器 计算机配置–管理模板–系统–windows时间服务–时间提供程序–启动windows NTP服务器
    管理员身份运行终端：
    输入：net stop w32Time，回车
    等待NTP服务停止。
    然后再输入：net start w32Time，回车
    启动NTP服务。
    a、按下 Win + R，输入 regedit 打开注册表编辑器，导航到以下路径：
    HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\W32Time\Parameters
    b、双击右侧的 Type 项，将值改为 NTP（默认是 NT5DS，表示域时间同步）
    c、导航到路径：HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\W32Time\Config
    d、双击 AnnounceFlags，将值改为 5（表示声明为可靠时间源）
    e、导航到路径：HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\W32Time\TimeProviders\NtpServer
    f、双击 Enabled，将值设为 1（启用 NTP 服务器）
    g、然后输入services.msc命令，确定。拖动滚动条，找到Windows Time。右键 Windows Time 服务，选择 重新启动。验证服务状态是否为 正在运行。（若重启失败，可直接重启电脑）
    h、右键 Windows Time选择属性、把启动类型修改为“自动”

    测试：在cmd窗口中输入w32tm /stripchart /computer:127.0.0.1 ，如果有回显则服务正常

配置防火墙允许 NTP 端口（可选）：
打开 控制面板 → Windows Defender 防火墙 → 高级设置。
右键 入站规则，选择 新建规则。
选择 端口 → UDP → 输入 123 → 允许连接 → 完成。

2、在Raspbian 配置，依次执行以下指令：
设置正确时区：
    sudo timedatectl set-timezone Asia/Shanghai  # 示例设为上海时区

方案一：使用默认的 systemd-timesyncd
sudo apt update
sudo apt install systemd-timesyncd
编辑配置文件：sudo nano /etc/systemd/timesyncd.conf
修改以下内容：
[Time]
NTP=192.168.20.89
#FallbackNTP=0.pool.ntp.org
RootDistanceMaxSec=30
#PollIntervalMinSec=32
#PollIntervalMaxSec=2048

安装完成后，启用并启动服务：
sudo systemctl enable systemd-timesyncd
sudo systemctl start systemd-timesyncd

检查服务状态：systemctl status systemd-timesyncd
检查当前的时间同步状态：timedatectl status

重启并手动同步：sudo systemctl restart systemd-timesyncd

重启服务并生效：sudo systemctl enable --now systemd-timesyncd
sudo systemctl disable systemd-timesyncd  禁用

方案二：
安装并配置 chrony（精准同步）：
依次执行：
a、安装 chrony：sudo apt update && sudo apt install chrony -y
b、编辑配置文件：sudo nano /etc/chrony/chrony.conf
c、注释或删除默认的NTP服务器，添加局域网NTP服务器：server 192.168.20.99 iburst  # 替换成NTP服务器IP（server ntp.aliyun.com iburst）
d、重启服务并设置开机自启：
    sudo systemctl restart chrony
    sudo systemctl enable chrony

确认 chrony 服务是否正常运行：sudo systemctl status chrony
查看日志：journalctl -u chrony -b  # 查看启动日志
查看时间源同步状态：chronyc -n sources
查看跟踪统计：chronyc -n tracking
手动触发时间同步：sudo chronyc makestep
将系统时间写入硬件时钟：sudo hwclock --systohc
确认树莓派能访问 NTP 服务器的 UDP 123 端口：nc -uzv 192.168.20.99 123
停止 Chrony 服务：sudo systemctl stop chrony
禁用开机自启：sudo systemctl disable chrony
卸载 Chrony 软件包：
sudo apt-get remove --purge chrony
sudo apt-get autoremove


手动设置时间指令：
date #查看当前时间
date -s "2016-03-31 10:18:00" #设置当前时间为2016年3月31日10:18:00
date -s 2016-03-31 #设置当前日期为2016年3月31日0:00:00
date -s 10:18:00　#设置当前时间为10:18:00





