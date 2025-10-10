# ICPAssetExpress V4.0

郑重声明：文中所涉及的技术、思路和工具仅供以安全为目的的学习交流使用，任何人不得将其用于非法用途以及盈利等目的，否则后果自行承担。

# <font style="color:rgb(31, 35, 40);">0x01 简介</font>
<font style="color:rgb(31, 35, 40);">一款互联网有效资产发现工具，方便快速对大量企业进行信息收集，快速打点</font>

<font style="color:rgb(31, 35, 40);">（目前基于360 quake会员api）</font>

---

**【交流讨论】欢迎各位大佬关注公众号、私聊讨论技术和工具改进思路，后期也会提供一些内测版本**

![image.png](https://cdn.nlark.com/yuque/0/2025/png/39031852/1760096678032-c6c59111-c8d1-4ea2-a8cf-d3e88879547e.png?x-oss-process=image%2Fformat%2Cwebp)

---

# <font style="color:rgb(31, 35, 40);">0x02 功能</font>
1. 调用 <font style="color:rgb(31, 35, 40);">360 quake api对目标企业关键词（icp_keywords）进行查询</font>
2. <font style="color:rgb(31, 35, 40);">调用 gogo </font><font style="color:rgb(31, 35, 40) !important;">v2.13.8</font><font style="color:rgb(31, 35, 40);"> 以及 observer_ward 对接口查询到的 ip 及 url 进行端口探测及指纹识别</font>
3. <font style="color:rgb(31, 35, 40);">调用 cdnAnalyzer及 fofa api 实现cdn过滤及ip反查（注意：已内置部分公共服务过滤规则）</font>
4. <font style="color:rgb(31, 35, 40);">实时查询工信部icp备案，获取目标企业所属web、小程序、app信息（支持第三方及本地部署）</font>
5. <font style="color:rgb(31, 35, 40);">在对应企业文件夹写入结果</font>

<img src="https://cdn.nlark.com/yuque/0/2025/png/39031852/1751248440805-9bead298-48b8-4083-838f-a42eaa85f3ca.png" style="zoom:67%;" />

```python
支持两种运行模式：

1. 基础模式 (-b, --basic):
   - Quake API 查询获取资产 (IP, URL等)
   - 将提取的IP和URL分别保存到 .txt 文件
   - 对从Quake获取的URL进行指纹识别 (observer_ward)
   - 结果整理输出 (Excel, .txt文件归档)

2. 高级模式 (-a, --advanced, 默认):
   - Quake API 查询获取资产
   - 对提取的IP进行gogo扫描 (端口、服务、简单漏洞探测)
   - 对从Quake获取的URL进行初次指纹识别
   - 对gogo发现的新URL进行二次指纹识别
   - 结果整理输出 (Excel, .txt文件归档)

其余辅助功能及可配置项见后续具体使用部分及-h
```

# <font style="color:rgb(31, 35, 40);">0x03 使用</font>
1. **按照observer_ward项目中所示运行observer_ward或手动更新指纹（不然无法进行指纹识别）**

项目地址： [https://github.com/emo-crab/observer_ward](https://github.com/emo-crab/observer_ward)

2. **<font style="color:rgb(38, 38, 38);">按照cdnAnalyzer项目中所示运行cdnAnalyzer或手动获取cdn数据源</font>**

项目地址：[https://github.com/winezer0/cdnAnalyzer](https://github.com/winezer0/cdnAnalyzer)

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1760064023530-8baecddf-8183-45c1-a9fb-5ee430571682.png)

可先启动 clash等代理，运行该命令等待执行完毕即可（下载数据源）

3. **<font style="color:rgb(38, 38, 38);">安装所需依赖</font>**

```plain
pip install -r requirements.txt 
```

4. **<font style="color:rgb(31, 35, 40);">各平台 api_key 、默认端口、基础语句模板、缓存有效期等参数可自行设置调整</font>**

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1760059943616-80a6953e-52e8-47b7-9b28-726b080ea495.png)

【注意】

<font style="color:rgb(31, 35, 40);">查询工信部备案数据（小程序、app）功能支持两种途径，对应两个版本，输出效果一致</font>

**<font style="color:rgb(31, 35, 40);">（1）需于镜芯接口平台获取Key，免费使用api接口</font>**

<font style="color:rgb(31, 35, 40);">接口平台地址：</font>[https://api2.wer.plus/user/register?cps=8nJjEdle](https://api2.wer.plus/user/register?cps=8nJjEdle)![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1760059988111-2da6c36e-867a-4682-b0b8-3aa8869955b5.png)

**（2）使用 icp_query 项目实现本地icp接口查询，需单独配置**	

​	a.  打开/tools/icp文件夹

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1760060356485-43f62f76-7b51-4537-b71f-250dd6069056.png) 

​	b.  根据ICP_Query项目进行配置，具体可详见该项目地址（代理池/出口有ipv6的网络）

  	（修改对应网卡名即可，若无法正常使用建议使用第三方调用版本）

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1760060446388-35f5e880-ae4b-4769-ac05-7acec3475a2b.png) 

​	c.  双击icp.bat启动本地icp查询服务，正常使用脚本

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1760060711605-d1514699-4066-4564-baa2-35d067edb5fa.png)

5. **在 icpCheck.txt 文件中按行写入目标单位关键词**

【注意】

+ 关键词会进行模糊匹配，如查询xx集团，则会查询出xx集团a公司、xx集团b公司...
+ <font style="color:rgb(31, 35, 40);">扫描 ip 端口为默认端口+目标公司查询结果端口去重</font>
+ 为避免终端输出信息过多，调用工具默认<font style="color:rgb(31, 35, 40);">静默模式，如需可自行添加 --showScanInfo 参数</font>
+ <font style="color:rgb(38, 38, 38);">内置资产预检机制，满足条件才获取全部数据，否则跳过该资产，可自行调整阈值</font>
    1. <font style="color:rgb(38, 38, 38);">quake请求筛选条件：单个语句请求资源5000条</font>
    2. <font style="color:rgb(38, 38, 38);">反查ip筛选条件：单个ip资产不超过300条</font>

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1760061676452-c30fef07-56b6-4057-9b1f-655fb49cbfc1.png)

+ 为避免垃圾数据和积分浪费，脚本中查询语句如下，可自行在配置处修改

PS：如果需要模糊匹配使更精确，可添加条件如 icp:"京icp"

```plain
(icp_keywords:"{target}" or cert:"{target}") and country:"China" AND not province:"Hongkong"
```

6. **<font style="color:rgb(31, 35, 40);">运行命令，可见详情</font>**

```plain
python ICPAssetExpress.py -h
```

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1760060890863-59697ab7-e0e7-4a1d-80d2-14bd24e09940.png)

```plain
# 建议开启 --skip-fofa-fingerprint（跳过对fofa反查ip获取url的指纹识别）
（ip反查存在共享服务未过滤完全情况，可能产生大量垃圾数据，导致指纹识别大幅降低效率）

【命令示例】
# 基础模式（仅扫描quake url），查询备案web、小程序及app，fofa反查ip（跳过fofa url识别）
python ICPAssetExpress.py -b -checkother "web,app,mapp" --skip-fofa-fingerprint -o 输出目录

# 高级模式（扫描quake url，调用gogo扫描ip），查询备案小程序，跳过fofa反查ip
python ICPAssetExpress.py -a -checkother "mapp" --no-fofa -o 输出目录

# 仅批量导出资产模式（仅根据quake备案导出资产，无需其他参数）
python ICPAssetExpress.py --onlyquake -o 输出目录

# 仅批量查询icp模式（仅根据传入目标icp批量查询备案资产，无需其他参数）
python ICPAssetExpress.py --onlyicp -o 输出目录

# merge脚本（遍历输出结果合并quake、指纹识别、gogo结果、ip反查结果）
python merge.py -t 存放结果目录 -o 输出目录

# process_results脚本（中断临时处理脚本，在异常结束情况下处理已有结果至正常状态）
python process_results.py -t 存放结果目录 -o 输出目录

【注意】
若未配置相关接口，不使用相关模块即可，具体如下：
● 未配置fofa key，可使用 --no-fofa 跳过fofa调用阶段
● 未配置第三方工信部备案数据查询接口 key，不添加 -checkother 即可跳过该功能
```

7. **以公司为单位输出结果，包含全部探测结果，具体内容自行查看**

【注意】

+ 需等脚本全部运行完毕才能正常获取结果
+ 过程中处理的 txt 文件存放在related materials文件夹中，对工具处理方式不满意可二次自行处理
+ 运行结束后会生成自查结果以及日志文件 log.txt，出现报错及查询失败可自行排查，
+ 通过自查结果可快速判断当前缓存所有资产

# <font style="color:rgb(31, 35, 40);">0x04 效果截图</font>
1. 输出、merge合并后目录效果

<img src="https://cdn.nlark.com/yuque/0/2025/png/39031852/1760078027650-f458471d-30d8-4839-82ab-4dcbcdb60933.png" style="zoom:67%;" />

2. icp备案查询输出结果（web、小程序、app）

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751218618812-2ec27e6c-e390-442f-b0ff-916b612d50ab.png)

3. 自查报告输出结果

<img src="https://cdn.nlark.com/yuque/0/2025/png/39031852/1751216925325-a1f92fc4-b086-4730-a219-25823d07fa4c.png" style="zoom:67%;" />![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751246597256-f9a6b26e-bf09-45ed-bdbb-6ccac0fdf36c.png)

4. 目标单位favicon查询语句结果

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1760078352024-622dec78-7b22-4b67-a8e2-0b355d514853.png)

5. merge脚本合并多个单位结果（部分效果）

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1754543813320-f007abcd-ad21-429f-bf98-9d81bcaa1d58.png)

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1754544110286-02ee718d-de6c-457e-a798-50a2b197fc31.png)

# <font style="color:rgb(31, 35, 40);">0x05 致谢</font>
本脚本是为提高工作效率用 AI 完成的缝合工具，感谢各位师傅的开源项目和提议！！

<font style="color:rgb(31, 35, 40);">gh0stkey</font>

<font style="color:rgb(31, 35, 40);">樱花庄的本间白猫</font>

[https://github.com/chainreactors/gogo](https://github.com/chainreactors/gogo)

[https://github.com/emo-crab/observer_ward](https://github.com/emo-crab/observer_ward)

[https://github.com/winezer0/cdnAnalyzer](https://github.com/winezer0/cdnAnalyzer)

[https://github.com/HG-ha/ICP_Query](https://github.com/HG-ha/ICP_Query)

[https://github.com/shadow1ng/fscan](https://github.com/shadow1ng/fscan)

[https://github.com/yz1639/fscanOutputBeautify](https://github.com/yz1639/FscanOutputBeautify)

# <font style="color:rgb(31, 35, 40);">0x06 更新记录</font>
**2025.5.19**

**添加结果合并脚本（merge.py）**

+ 应用场景：同一集团下大量相关企业，结果按公司输出至文件较为繁琐，且可能大部分公司发现资产很少
+ 用途：合并指定目录下所有quake资产、指纹识别、端口扫描结果，同时标注数据来源表格

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1747646993640-19ac82c1-afd9-4187-b1ab-f789ccfc9bd9.png)

**2025.6.29**

1. **添加 fofa（ip反查）及其他备案资产（app、小程序）查询功能**
2. **添加全部查询结果的缓存机制**
3. **添加自查机制，运行完毕输出本次运行状态及缓存所有资产便于自行梳理**
4. **添加结果合并脚本（merge.py）的fofa结果合并功能**

**2025.8.25**

1. **添加无主动探测仅quake资产导出功能**
2. **完善结果合并脚本（merge.py）的gogo结果合并功能**
3. **添加中断临时处理脚本（process_results.py），若工具卡死，手动退出后可处理已有结果**
4. **重构端口扫描功能，端口扫描从fscan改用gogo（补充端口指纹、协议识别）**
5. **完善、优化输出文件效果**

**2025.10.10**

1. **实现icp查询接口的本地化**
2. **添加仅icp批量查询功能（web、app、小程序）**
3. **新增资产获取预检机制，避免单个大资产浪费过量积分**
4. **配合cdn过滤工具及资产获取预检机制，大幅提升ip反查能力**
5. **优化quake语句模板，增强隐藏资产发现能力**
6. **新增favicon查询语句，一键发现目标单位隐藏资产及供应链资产**
7. **添加host碰撞功能模块（实验性功能）**
8. **优化接口报错处理逻辑，提升脚本稳定性**

