# ICPAssetExpress

+ <font style="color:rgb(31, 35, 40);">郑重声明：文中所涉及的技术、思路和工具仅供以安全为目的的学习交流使用，任何人不得将其用于非法用途以及盈利等目的，否则后果自行承担。</font>

<h1 id="LnNvS"><font style="color:rgb(31, 35, 40);">0x01 简介</font></h1>

<font style="color:rgb(31, 35, 40);">一款互联网有效资产发现工具，方便快速对多个企业进行信息收集（目前基于360 quake api）</font>

<h1 id="xw5kp"><font style="color:rgb(31, 35, 40);">0x02 功能</font></h1>

1. 调用 <font style="color:rgb(31, 35, 40);">360 quake api对目标企业关键词（icp_keywords）进行查询</font>
2. <font style="color:rgb(31, 35, 40);">调用 fscan1.8.2 以及 observer_ward 对接口查询到的 ip 及 url 进行端口探测及指纹识别</font>
3. <font style="color:rgb(31, 35, 40);">在对应企业文件夹写入结果</font>

```python
支持两种运行模式：

1. 基础模式 (-b, --basic):
   - Quake API 查询获取资产 (IP, URL等)
   - 将提取的IP和URL分别保存到 .txt 文件
   - 对从Quake获取的URL进行指纹识别 (observer_ward)
   - 结果整理输出 (Excel, .txt文件归档)

2. 高级模式 (-a, --advanced, 默认):
   - Quake API 查询获取资产
   - 对提取的IP进行fscan扫描 (端口、服务、简单漏洞探测)
   - 对从Quake获取的URL进行初次指纹识别
   - 对fscan发现的新URL进行二次指纹识别
   - 结果整理输出 (Excel, .txt文件归档)

可配置项包括：Quake API Key, 输入文件, 输出目录, Quake查询语句模板, 是否显示扫描实时输出等。
```

<h1 id="sTkQf"><font style="color:rgb(31, 35, 40);">0x03 使用</font></h1>

1. **按照observer_ward项目中所示运行observer_ward或手动更新指纹（不然无法进行指纹识别）**

项目地址： [https://github.com/emo-crab/observer_ward](https://github.com/emo-crab/observer_ward)

2. **安装所需依赖**

```plain
pip install -r requirements.txt 
```

3. **在 icpCheck.txt 文件中按行写入目标单位关键词**

【注意】

+ 关键词会进行模糊匹配，如查询xx集团，则会查询出xx集团a公司、xx集团b公司...

4. **<font style="color:rgb(31, 35, 40);">基本配置中 api_key 、默认端口、基础语句模板等参数可自行设置调整</font>**

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1747117242964-1cfdf7ee-5cc5-4ddd-8323-6bab8cdfcbe2.png)

【注意】

+ <font style="color:rgb(31, 35, 40);">扫描 ip 端口为默认端口+目标公司查询结果端口去重</font>
+ 为避免终端输出信息过多，调用工具默认<font style="color:rgb(31, 35, 40);">静默模式，如需可自行添加 --showScanInfo 参数</font>
+ 为避免垃圾数据和积分浪费，脚本中查询语句如下，可自行在配置处修改

PS：如果需要模糊匹配使更精确，可添加条件如 icp:"京icp"

```plain
icp_keywords:"{target}" and not domain_is_wildcard:true and country:"China" AND not province:"Hongkong"
```

5. **<font style="color:rgb(31, 35, 40);">运行命令，可见详情</font>**

```plain
python ICPAssetExpress.py -h
```

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1747227996472-b3b7bcec-0140-49c4-a53f-b32d615393fb.png)

6. **<font style="color:rgb(31, 35, 40);">输出结果（逻辑小缺陷：需等脚本全部运行完毕才能正常获取结果，后续可能会进行调整）</font>**

（1）将以公司为单位输出结果，包含全部探测结果，具体内容自行查看

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1747118481893-d4f99cb6-4dd4-4c8d-843c-3b44881dcab6.png)

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1747129638610-3796b18b-4bb8-4c8d-8f8e-d6dfcd41e59d.png)

结果文件中分别为各阶段扫描结果，基本逻辑如下：

（先完全走完step1，再走step2，不太会画图多见谅）

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1747120430519-230ccdd5-5dc4-45fd-9642-f754f08efc9c.png)

（2）过程处理的txt文件存放在related materials文件夹中，对工具处理方式不满意可二次自行处理

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1747118748114-277539a3-ba71-477d-9450-384390d97089.png)

（3）运行结束后项目目录会出现日志文件 log.txt，出现报错或查询失败可自行根据日志排查

<h1 id="hKjp6"><font style="color:rgb(31, 35, 40);">0x04 运行截图</font></h1>

![img](https://cdn.nlark.com/yuque/0/2025/png/39031852/1747389977368-a9e087b8-e15d-4904-89e3-e30c02a981b7.png)

![img](https://cdn.nlark.com/yuque/0/2025/png/39031852/1747390134236-84f51d18-2cce-4219-ab54-32c1e9b911bd.png)

![img](https://cdn.nlark.com/yuque/0/2025/png/39031852/1747390183946-87d84564-2047-4708-ab96-52534f6959a1.png)

<h1 id="L0UAF"><font style="color:rgb(31, 35, 40);">0x05 致谢</font></h1>

本脚本是为提高工作效率用 AI 完成的缝合工具，感谢各位师傅的开源项目和提议！！

<font style="color:rgb(31, 35, 40);">gh0stkey</font>

<font style="color:rgb(31, 35, 40);">樱花庄的本间白猫</font>

[https://github.com/emo-crab/observer_ward](https://github.com/emo-crab/observer_ward)

[https://github.com/shadow1ng/fscan](https://github.com/shadow1ng/fscan)

[https://github.com/yz1639/FscanOutputBeautify](https://github.com/yz1639/FscanOutputBeautify)

