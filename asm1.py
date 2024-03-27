from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import col, to_date, udf, explode
import re

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('MyApp') \
    .config("spark.jars.packages", 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config('spark.mongodb.input.uri', 'mongodb://root:root@localhost/dep303x?authSource=admin') \
    .config('spark.mongodb.output.uri', 'mongodb://root:root@localhost/dep303x?authSource=admin') \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Tạo dataframe questions
questions_df = spark.read\
        .format('com.mongodb.spark.sql.DefaultSource')\
        .option("collection", "questions")\
        .option("inferSchema", "true")\
        .load()

# Tạo dataframe answers
answers_df = spark.read\
        .format('com.mongodb.spark.sql.DefaultSource')\
        .option("collection", "answers")\
        .option("inferSchema", "true")\
        .load()

# Chuyển đổi kiểu dữ liệu dataframe questions_df
df_questions = questions_df.withColumn("Id", col("Id").cast("integer")) \
       .withColumn("OwnerUserId", col("OwnerUserId").cast("integer")) \
       .withColumn("CreationDate", to_date(col("CreationDate"), "yyyy-MM-dd")) \
       .withColumn("ClosedDate", to_date(col("ClosedDate"), "yyyy-MM-dd"))

# Chuyển đổi kiểu dữ liệu ở dataframe answers_df
df_answers = answers_df.withColumn("Id", col("Id").cast("integer")) \
       .withColumn("OwnerUserId", col("OwnerUserId").cast("integer")) \
       .withColumn("CreationDate", to_date(col("CreationDate"), "yyyy-MM-dd"))

def yeu_cau_1():
        # Trích xuất dữ liệu từ column Body
        body_df = df_questions.select("Body")

        def extract_languages(context):
                programming_languages = r"Java\b|Python|C\+\+|C\#|Go|Ruby|Javascript|PHP|HTML|CSS|SQL"
                array_languages = re.findall(programming_languages, context)

                if context is not None:
                        return array_languages
                
        # Định nghĩa UDF
        extracter = udf(lambda z: extract_languages(z), ArrayType(StringType()))

        # Áp dụng hàm định nghĩa cho cột extracter và đặt tên kết quả là Languages_array
        body_df_with_counts = body_df.withColumn("Languages_array", extracter(col("Body")))
        
        # Tạo một dataframe mới từ cột Languages_array
        LanguageCounts_df = body_df_with_counts.select(explode("Languages_array").alias("languages"))
        
        # Tính tổng số lần xuất hiện của mỗi ngôn ngữ
        language_counts = LanguageCounts_df.groupBy("languages").count()

        return language_counts

def yeu_cau_2():
        # Define regex pattern
        domain_pattern = r'"https?://([^/]+)'

        # Define a function to extract domains from text using regex
        def extract_domains(text):
                matches = re.findall(domain_pattern, text)
                if matches:
                        return matches

        # Create a user-defined function (UDF)
        extract_domains_udf = udf(extract_domains, ArrayType(StringType()))

        # Trích xuất dữ liệu từ cột "Body"
        body_df = df_questions.select("Body")

        # Áp dụng UDF để trích xuất domain cho mỗi dòng trong cột "Body"
        body_with_domains = body_df.withColumn("Domains", extract_domains_udf(body_df["Body"]))

        # Sử dụng explode() để tách các phần tử trong mảng "Domains" thành các hàng riêng biệt
        body_exploded = body_with_domains.select(explode("Domains").alias("Domain"))

        # Đếm số lần xuất hiện của mỗi tên miền
        domain_count = body_exploded.groupBy("Domain").count().sort("count", ascending=False)

        # Danh sách các giá trị Domain cần lựa chọn
        selected_domains = ["www.cs.bham.ac.uk", "groups.csail.mit.edu", "fiddlertool.com", "www.dynagraph.org", "images.mydomain.com", "img7.imageshack.us"]


        # Lọc các dòng có trường "Domain" nằm trong danh sách selected_domains
        selected_rows = domain_count.filter(col("Domain").isin(selected_domains))

        return selected_rows

def yeu_cau_3():
        # Trích xuất dữ liệu từ column OwnerUserId, CreationDate, Score ở dataframe questions
        questions_df = df_questions.select("OwnerUserId", "CreationDate", "Score")

        # Trích xuất dữ liệu từ column OwnerUserId, CreationDate, Score ở dataframe answers
        answers_df = df_answers.select("OwnerUserId", "CreationDate", "Score")

        # Xử lý dữ liệu ở dataframe questions
        data_questions = questions_df.groupBy("OwnerUserId","CreationDate") \
                                .sum("Score").withColumnRenamed("sum(Score)", "TotalScore")

        # Xử lý dữ liệu ở dataframe answers
        data_answers = answers_df.groupBy("OwnerUserId","CreationDate").sum("Score") \
                                .withColumnRenamed("sum(Score)", "TotalScore")
        
        # Nối hai DataFrame theo chiều dọc
        result = data_questions.unionAll(data_answers)
        
        date_total_score = result.groupBy("OwnerUserId", "CreationDate").sum("TotalScore") \
                                 .withColumnRenamed("sum(TotalScore)", "TotalScore")

        # Sắp xếp theo trường OwnerUserId và CreationDate
        date_total_score = date_total_score.orderBy("OwnerUserId", "CreationDate")

        # Loại bỏ các hàng chứa giá trị null trong cột "OwnerUserId"
        date_total_score = date_total_score.filter(col("OwnerUserId").isNotNull())

        return date_total_score

def yeu_cau_4():
        start_date = '2008-01-01'
        end_date = '2009-01-01'

        data_frame = yeu_cau_3()
        
        # Lọc theo khoảng thời gian
        data_frame = data_frame.filter((col("CreationDate") >= start_date) & (col("CreationDate") <= end_date))

        # Tính tổng theo dataframe trên
        data_frame = data_frame.groupBy("OwnerUserId").sum("TotalScore").withColumnRenamed("sum(TotalScore)", "TotalScore")

        return data_frame

def yeu_cau_5():
        # Lấy dữ liệu từ data answers Id, ParentId
        data_answers = df_answers.select("Id", "ParentId")

        # Xử lý dữ liệu
        data_answers = data_answers.groupBy("ParentID").count().withColumnRenamed("ParentID", "questions_id") \
                                .filter(col('count') > 5) \
                                .orderBy("questions_id")

        return data_answers

def yeu_cau_6():
        # Ta cần kiểm tra từng yêu cầu: 
        # yêu cầu Có nhiều hơn 50 câu trả lời
        # tổng số điểm đạt được khi trả lời lớn hơn 500
        # Có nhiều hơn 5 câu trả lời ngay trong ngày câu hỏi được tạo


        # Những người có nhiều hơn 50 câu trả lời
        answers_by_user = df_answers.groupBy("OwnerUserId").count() \
                                .filter(col("count") > 50)

        # Những người có tổng số điểm đạt được khi trả lời lớn hơn 500
        user_by_score = df_answers.groupBy("OwnerUserId").sum("Score") \
                                .withColumnRenamed("sum(Score)", "totalScore") \
                                .filter(col("totalScore") > 500)
        
        # Những người có nhiều hơn 5 câu trả lời ngay trong ngày câu hỏi được tạo
        # Điều kiện join giữa dataframe questions và answers
        join_expr = df_questions["Id"] == df_answers["ParentId"]

        df_join = df_questions.join(df_answers, join_expr, "inner") \
                        .select(df_questions["Id"].alias("id_question"),
                                df_questions["CreationDate"].alias("date_question"),
                                df_questions["OwnerUserId"].alias("id_user_question"),
                                df_answers["Id"].alias("id_answer"),
                                df_answers["OwnerUserId"].alias("id_user_answer"),
                                df_answers["CreationDate"].alias("date_answer"),
                                )
        
        df_join = df_join.filter(col("date_question") == col("date_answer")) \
                         .groupBy("id_question", "id_user_question").count() \
                         .filter(col("count") > 5)
        
        # Những người đang có trạng thái Active User
        # Lọc ra danh sách các id_user_question từ df_join
        id_user_question_list = df_join.select("id_user_question").distinct() \
                                        .withColumnRenamed("id_user_question", "Id_user")
        # Lọc ra danh sách các OwnerUserId từ answers_by_user
        answers_user_list = answers_by_user.select("OwnerUserId").distinct() \
                                           .withColumnRenamed("OwnerUserId", "Id_user")
        # Lọc ra danh sách các OwnerUserId từ user_by_score
        score_user_list = user_by_score.select("OwnerUserId").distinct() \
                                       .withColumnRenamed("OwnerUserId", "Id_user")

        # Lọc những người dùng có Id_user nằm trong danh sách id_user_question từ df_join
        users = id_user_question_list.union(answers_user_list) \
                                     .union(score_user_list) \
                                     .orderBy("Id_user")

        return users
        
def main():
        # Tạo Menu
        while True:
                print("_________________________________Menu_________________________________________")
                print("|""1. Yêu cầu 1: Tính số lần xuất hiện của các ngôn ngữ lập trình                |")
                print("|""2. Yêu cầu 2: Tìm các domain được sử dụng nhiều nhất trong các câu hỏi        |")
                print("|""3. Yêu cầu 3: Tính tổng điểm của User theo từng ngày                          |")
                print("|""4. Yêu cầu 4: Tính tổng số điểm mà User đạt được trong một khoảng thời gian   |")
                print("|""5. Yêu cầu 5: Tìm các câu hỏi có nhiều câu trả lời                            |")
                print("|""6. Yêu cầu 6: Tìm các Active User                                             |")
                print("|""0. Exit                                                                       |")
                print("_" * 78)

                choose = input("Choose Your Select: ")

                if choose == '1':
                    
                        print("Số lần xuất hiện của các ngôn ngữ lập trình")
                        yeu_cau_1().show()  

                elif choose == '2':

                        print("Các domain được sử dụng nhiều nhất trong các câu hỏi")
                        yeu_cau_2().show()
                
                elif choose == '3':

                        print("Tổng điểm của User theo từng ngày")
                        yeu_cau_3().show()

                elif choose == '4':

                        print("Tổng số điểm mà User đạt được trong một khoảng thời gian START = '01-01-2008' và END = '01-01-2009'")
                        yeu_cau_4().show()

                elif choose == '5':

                        print("Các câu hỏi có nhiều câu trả lời")
                        yeu_cau_5().show()

                elif choose == '6':

                        print("Các Active User")
                        yc_6 = yeu_cau_6()
                        yc_6.show()

                        count_users = yc_6.count()
                        print("\n")
                        print(f"Số Users Active là: {count_users}")

                elif choose == '0':
                        print("Thanks (^_^) ")
                        break

                # Trường hợp nhập sai khi chọn ở menu
                else:
                        print("Mời bạn nhập đúng giá trị từ 0 -> 6. ")
                        continue



if __name__ == '__main__':
        main()



