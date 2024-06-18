import logging
import json
import azure.functions as func
import uuid
# 이 코드에서 클라이언트에서 Access-Control-Allow-Origin이 필요하지 않게 바꿔줘 
def main(req: func.HttpRequest) -> func.HttpResponse:
    headers = {
        "Access-Control-Allow-Origin": "*",  # 혹은 특정 origin을 지정
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type"
    }

    if req.method == "OPTIONS":
        return func.HttpResponse(status_code=200, headers=headers)

    data = req.get_json()
    
    logging.info(f"Received JSON data: {json.dumps(data, ensure_ascii=False)}")
#
    if data is not None:
        result_json = {
            "iso_cd": data["iso_cd"],
            "language": data["language"],
            "product_code": data["product_code"],
            "product_model_code": data["product_model_code"],
            "product_group_code" : data["product_group_code"],
            "chatid" : str(uuid.uuid4()),
            }
        output = json.dumps(result_json, ensure_ascii=False)
        return func.HttpResponse(output)
    else:
        return func.HttpResponse(
            "파라미터 잘못호출했습니다. 다시한번 확인해주세요.",
            status_code=400
        )
