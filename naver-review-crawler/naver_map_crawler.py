from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import json
import platform
import sys
import os
import random
from datetime import datetime

class NaverMapCrawler:
    def __init__(self):
        # Windows 환경에서 한글 출력을 위한 설정
        if sys.platform == 'win32':
            os.system('chcp 65001')

        self.chrome_options = Options()
        # self.chrome_options.add_argument('--headless')  # 헤드리스 모드 비활성화
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--start-maximized')
        self.chrome_options.add_argument('--window-size=1920,1080')
        
        # 봇 감지 회피를 위한 추가 옵션들
        self.chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        self.chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # User-Agent 설정 (Windows 추가)
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        self.chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')
        
        # M1/M2 Mac용 추가 옵션
        if platform.processor() == 'arm':
            self.chrome_options.add_argument('--disable-gpu')
            self.chrome_options.add_argument('--disable-software-rasterizer')
        
        self.driver = webdriver.Chrome(options=self.chrome_options)
        
        # 봇 감지 회피를 위한 JavaScript 실행
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    def wait_and_switch_to_iframe(self, iframe_css_selector, timeout=10):
        """iframe을 기다리고 전환"""
        try:
            # 기존 iframe에서 빠져나오기
            self.driver.switch_to.default_content()
            # 새 iframe 기다리기
            WebDriverWait(self.driver, timeout).until(
                EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, iframe_css_selector))
            )
            return True
        except Exception as e:
            print(f"iframe 전환 실패 ({iframe_css_selector}): {e}")
            return False
            
    def search_places(self, university_name):
        """대학교 주변 식당/카페 검색"""
        results = []
        base_url = f"https://map.naver.com/p/search/{university_name}%20카페"
        self.driver.get(base_url)
        time.sleep(3)  # 초기 로딩 대기
        
        # 검색 결과 iframe으로 전환
        if not self.wait_and_switch_to_iframe("#searchIframe"):
            print("검색 결과 iframe을 찾을 수 없습니다.")
            return results
            
        # 검색 결과 목록 대기
        try:
            print("검색 결과 대기 시작")
            # 검색 결과가 로드될 때까지 대기
            WebDriverWait(self.driver, 2).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div#_pcmap_list_scroll_container"))
            )
            time.sleep(2)  # 추가 대기
            print("검색 결과 대기 완료")
            # 결과가 더 로드될 때까지 스크롤
            scroll_container = self.driver.find_element(By.CSS_SELECTOR, "div#_pcmap_list_scroll_container")
            print("스크롤 시작")
            last_height = self.driver.execute_script("return arguments[0].scrollHeight", scroll_container)
            scroll_attempts = 0
            max_attempts = 10
            
            while scroll_attempts < max_attempts:
                # 스크롤 다운
                self.driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight)", scroll_container)
                time.sleep(2)  # 스크롤 후 대기 시간 증가
                
                # 새로운 높이 계산
                new_height = self.driver.execute_script("return arguments[0].scrollHeight", scroll_container)
                if new_height == last_height:
                    second_try_height = self.driver.execute_script("return arguments[0].scrollHeight", scroll_container)
                    if second_try_height == new_height:
                        break
                last_height = new_height
                scroll_attempts += 1
            
            # 모든 결과가 로드될 때까지 대기
            time.sleep(2)
            
            # 장소 목록 가져오기
            place_elements = self.driver.find_elements(By.CSS_SELECTOR, "li.UEzoS:not(.cZnHG), li.VLTHu:not(.cZnHG)")[:50]  # cZnHG 클래스 제외, 최대 50개로 제한
            print(f"발견된 장소 수: {len(place_elements)}")
            
            if not place_elements:
                print("장소 목록을 찾을 수 없습니다. 페이지 소스:")
                print(self.driver.page_source)
                return results
            
            for idx, place in enumerate(place_elements, 1):
                try:
                    # 장소 클릭
                    place_name_element = place.find_element(By.CSS_SELECTOR, "div.place_bluelink")
                    self.driver.execute_script("arguments[0].click();", place_name_element)
                    time.sleep(3)
                    
                    # 상세정보 iframe으로 전환
                    if not self.wait_and_switch_to_iframe("#entryIframe"):
                        continue
                    
                    # 상세 정보 추출
                    place_info = {
                        "가게이름": "",
                        "가게 평균 평점": "평점 없음",
                        "가게 리뷰": []
                    }
                    
                    try:
                        place_info["가게이름"] = self.driver.find_element(By.CSS_SELECTOR, "span.GHAhO").text
                    except:
                        continue
                        
                    try:
                        raw_rating = self.driver.find_element(By.CSS_SELECTOR, "span.PXMot.LXIwF").text
                        place_info["가게 평균 평점"] = raw_rating.strip().split('\n')[-1]
                    except:
                        pass

                    print(f"\n[{idx}/{len(place_elements)}] 가게 정보:")
                    print(f"이름: {place_info['가게이름']}")
                    print(f"평점: {place_info['가게 평균 평점']}")
                    
                    # 리뷰 버튼 찾기 및 클릭
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, '.flicking-camera > a')

                        # 'span.veBoZ'를 포함하고 텍스트에 '리뷰'가 있는 요소 필터링
                        filtered_elements = [
                            element for element in elements
                            if element.find_element(By.CSS_SELECTOR, 'span.veBoZ').text.find('리뷰') != -1
                        ]

                        review_tab = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable(filtered_elements[0])
                        )

                        self.driver.execute_script("arguments[0].click();", review_tab)
                        time.sleep(2)
                        
                        # 리뷰 컨테이너 스크롤
                        review_container = self.driver.find_element(By.CSS_SELECTOR, "div.place_section_content > ul")
                        last_review_height = 0
                        
                        print("리뷰 불러오는 중")
                        # 리뷰 더 보기 버튼이 있다면 클릭
                        more_button_clicks = 0  # 더보기 버튼 클릭 카운터
                        while more_button_clicks < 9:  # 최대 9번까지만 클릭 -> 최대 리뷰 100개
                            try:
                                more_button = self.driver.find_element(By.CSS_SELECTOR, "a.fvwqf")
                                self.driver.execute_script("arguments[0].click();", more_button)
                                time.sleep(0.5)
                                more_button_clicks += 1
                            except:
                                print("더 이상 불러올 리뷰가 없습니다.")
                                break

                        print('리뷰 불러오기 완료')
                        
                        # 리뷰 요소들 가져오기
                        review_elements = self.driver.find_elements(By.CSS_SELECTOR, "li.pui__X35jYm")
                        print('리뷰 개수:', len(review_elements))
                        
                        for review in review_elements:
                                
                            try:
                                try:
                                    tag_button = self.driver.find_element(By.CSS_SELECTOR, "a.pui__jhpEyP.pui__ggzZJ8")
                                    self.driver.execute_script("arguments[0].click();", tag_button)
                                except:
                                    pass
                                
                                review_info = {
                                    "리뷰어 리뷰수": [elem.text.replace("리뷰 ", "") for elem in review.find_elements(By.CSS_SELECTOR, "span.pui__WN-kAf") if "리뷰" in elem.text][0],
                                    "리뷰 텍스트": review.find_element(By.CSS_SELECTOR, "a.pui__xtsQN-").text,
                                    "리뷰 날짜": review.find_element(By.CSS_SELECTOR, "span.pui__gfuUIT > time").text,
                                    "가게 방문수": next((
                                        span.text.replace("번째 방문", "").strip() 
                                        for span in review.find_elements(By.CSS_SELECTOR, "span.pui__gfuUIT")
                                        if "번째 방문" in span.text
                                    ), "방문 정보 없음"),
                                    "태그들": [tag.text for tag in review.find_elements(By.CSS_SELECTOR, "span.pui__jhpEyP")]
                                }

                                place_info["가게 리뷰"].append(review_info)
                            except Exception as e:
                                print(f"리뷰 추출 중 에러: {e}")
                                continue
                                
                    except Exception as e:
                        print(f"리뷰 탭 처리 중 에러: {e}")
                    
                    results.append(place_info)
                    
                    # 검색 결과 iframe으로 돌아가기
                    if not self.wait_and_switch_to_iframe("#searchIframe"):
                        break
                        
                except Exception as e:
                    print(f"장소 처리 중 에러: {e}")
                    continue
                    
        except Exception as e:
            print(f"검색 결과 처리 중 에러: {e}")
            
        return results
        
    def save_to_json(self, data, filename="reviews.json"):
        """결과를 JSON 파일로 저장"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
    def close(self):
        """브라우저 종료"""
        self.driver.quit()

def main():
    university_name = input("대학교 이름을 입력하세요: ")
    crawler = NaverMapCrawler()
    try:
        results = crawler.search_places(university_name)
        # 파일명에 학교 이름과 현재 시간 추가
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{university_name}_reviews_{current_time}.json"
        crawler.save_to_json(results, filename)
        print(f"크롤링이 완료되었습니다. 결과는 {filename} 파일에 저장되었습니다.")
    finally:
        crawler.close()

if __name__ == "__main__":
    main() 