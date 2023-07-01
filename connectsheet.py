import gspread
import pandas as pd
import numpy as np

from oauth2client.service_account import ServiceAccountCredentials

from calculate import Calculate

# 구글 드라이브 인증 키 전역 변수
credentials_file = '../googlesheetKey/googlekey.json'  # 다운로드한 인증 정보 파일의 경로

# connect google sheet
class GoogleSheetHandler:

    def __init__(self,sheetTitle, sheetName):
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, self.scope)
        self.client = gspread.authorize(self.credentials)

        # Google 스프레드시트 열기
        self.spreadsheet = self.client.open(sheetTitle)  # 스프레드시트의 제목을 입력 -> 스프레트시트 설정.
        self.worksheet = self.spreadsheet.worksheet(sheetName)  # 시트 선택 (시트의 이름을 작성) -> 시트 설정

    # 행 아래로 데이터 집어넣는 함수
    def send_data(self, data):

        try:
            self.worksheet.append_rows(data)
            print("완료")
        except Exception as e:
            print(e)

    def getSingleRowData(self, row_number):
        self.row_data = self.worksheet.row_values(row_number)
        print(self.row_data)
        return self.row_data
    
    def getSingleColData(self, column_number):
        self.column_data = self.worksheet.col_values(column_number)
        print(self.column_data)
        return self.column_data
    
    # 스프레드 시트의 모든 데이터를 가져오는 함수
    def get_all_data(self):
        data = self.worksheet.get_all_values()
        return data
    
    # 1행의 데이터 이름을 기반으로 열의 데이터를 가져오는 함수
    def getColDataName(self, column_name):
        header_row = self.worksheet.row_values(1)  # 이름이 있는 첫 번째 행 가져오기

        if column_name in header_row:
            column_index = header_row.index(column_name) + 1  # 열 인덱스 계산
            self.column_data = self.worksheet.col_values(column_index)  # 열의 데이터 가져오기

            # 첫 번째 행은 이름이므로 데이터에서 제외
            self.column_data = self.column_data[1:]

            return self.column_data
        else:
            print("해당 이름을 가진 열을 찾을 수 없습니다.")
            return []
    
    # 데이터 가공하여 새로운 열 추가하기
    def addProcessedData(self, input_column_name, output_column_name, period):
        # 원본 데이터 가져오기
        input_data = self.getColDataName(input_column_name)
        if not input_data:
            print("원본 데이터를 가져올 수 없습니다.")
            return

        # EMA 계산
        calculator = Calculate(input_data)
        processed_data = calculator.ema(period)

        # 새로운 데이터 열 생성
        new_column_data = [None] * (len(input_data) - len(processed_data)) + processed_data

        # 기존 데이터 열 가져오기
        existing_column_data = self.getColDataName(output_column_name)
        if not existing_column_data:
            existing_column_data = []

        # 기존 데이터와 새로운 데이터 합치기
        merged_column_data = existing_column_data + new_column_data

        # 데이터 프레임 생성
        df = pd.DataFrame({
            input_column_name: input_data,
            output_column_name: merged_column_data
        })

        # 데이터 프레임을 리스트로 변환하여 시트에 전송
        data_to_send = df.values.tolist()
        self.send_data(data_to_send)


# 실행 예시
# dataframe = [['이름', '테스트 나이'], ['Alice', 25], ['Bob', 30]]
# gs = GoogleSheetHandler(dataframe, 'historical_data', 'test')
# gs.send_data()

sheet_handler = GoogleSheetHandler("historical_data", "BTC_5m")
sheet_handler.addProcessedData("Close", "EMA", 200)