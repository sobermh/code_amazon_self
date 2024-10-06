import json
json_str = """
{"id":44,"code":"f61a81c888cd","name":"AMCap","subTitle":"AMCap","desc":"AMCap视频采集卡","publishTime":null,"categoryId":6,"categoryName":"三方应用","icon":"https://kmoke.ver.cn:8169/cloud/product/img/2024/08/01/a0a8551e-e6ba-4759-bb9c-7abdb44fe4d4.png","image":"https://kmoke.ver.cn:8169/cloud/default/default.png","price":0,"vipPrice":0,"supportPlatform":"win","supportLanguage":"中文","status":1,"size":"3MB","sizeValue":"3","sizeUnit":"MB","describe":"AMCap视频采集卡","ext":{"system":"window","cpu":"i7","gpu":"RTX1080","directx":"无","ram":"16GB"},"sort":0,"insertTime":"2024-08-01 18:01:36","updateTime":"2024-08-07 21:30:40","version":"1.0.2",
"versionInfos":[{"id":62,"code":"f61a81c888cd001","productId":44,"name":"1.0.1","status":1,"insertTime":"2024-08-02 15:57:30","updateTime":"2024-08-02 16:24:32","desc":"首次发布","conf":{"install":"AMCap","start":"AMCap/AMCap.exe","require":"AMCap"},"fileUrl":"","fileSize":""},{"id":66,"code":"f61a81c888cd002","productId":44,"name":"1.0.2","status":1,"insertTime":"2024-08-02 17:38:32","updateTime":"2024-08-02 17:38:32","desc":"采集卡版本更新","conf":{"install":"AMCap","start":"AMCap\\AMCap.exe","require":"AMCap"},"fileUrl":"","fileSize":""}]}
"""


json_str2 = """
{"id": 44,"code": "f61a81c888cd","name": "AMCap","subTitle": "AMCap","desc": "AMCap\u89c6\u9891\u91c7\u96c6\u5361","publishTime": null,"categoryId": 6,
    "categoryName": "\u4e09\u65b9\u5e94\u7528",
    "icon": "https://kmoke.ver.cn:8169/cloud/product/img/2024/08/01/a0a8551e-e6ba-4759-bb9c-7abdb44fe4d4.png",
    "image": "https://kmoke.ver.cn:8169/cloud/default/default.png",
    "price": 0,
    "vipPrice": 0,
    "supportPlatform": "win",
    "supportLanguage": "\u4e2d\u6587",
    "status": 1,
    "size": "3MB",
    "sizeValue": "3",
    "sizeUnit": "MB",
    "describe": "AMCap\u89c6\u9891\u91c7\u96c6\u5361",
    "ext": {
        "system": "window",
        "cpu": "i7",
        "gpu": "RTX1080",
        "directx": "\u65e0",
        "ram": "16GB"
    },
    "sort": 0,
    "insertTime": "2024-08-01 18:01:36",
    "updateTime": "2024-08-07 21:30:40",
    "version": "1.0.2",
"versionInfos": [{"id": 62,"code": "f61a81c888cd001","productId": 44,"name": "1.0.1","status": 1,"insertTime": "2024-08-02 15:57:30","updateTime": "2024-08-02 16:24:32",
            "desc": "\u9996\u6b21\u53d1\u5e03",
            "conf": {
                "install": "AMCap",
                "start": "AMCap/AMCap.exe",
                "require": "AMCap"
            }
        },
        {
            "id": 66,
            "code": "f61a81c888cd002",
            "productId": 44,
            "name": "1.0.2",
            "status": 1,
            "insertTime": "2024-08-02 17:38:32",
            "updateTime": "2024-08-02 17:38:32",
            "desc": "\u91c7\u96c6\u5361\u7248\u672c\u66f4\u65b0",
            "conf": {
                "install": "AMCap",
                "start": "AMCap\\AMCap.exe",
                "require": "AMCap"
            }
        }
    ]
}
"""

print(json_str == json_str2)
