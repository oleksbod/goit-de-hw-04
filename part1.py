from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .appName("MyGoitSparkSandbox_Part1") \
    .getOrCreate()

# Завантажуємо датасет
nuek_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv('./nuek-vuh3.csv')

# Репартиціювання
nuek_repart = nuek_df.repartition(2)

# Пайплайн обробки
nuek_processed = nuek_repart \
    .where("final_priority < 3") \
    .select("unit_id", "final_priority") \
    .groupBy("unit_id") \
    .count()

# Додано фільтр
nuek_processed = nuek_processed.where("count > 2")

# Action — запускає DAG
nuek_processed.collect()

input("Press Enter to continue...")

# Закриваємо сесію Spark
spark.stop()

