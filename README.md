# CollectSet-and-Count  
How to collect set and count in Group by and Agg
# Input and Output Data
![Input data](https://github.com/user-attachments/assets/d47414af-40ed-4b7e-8869-dc6136a87bf2)


# Below is code  
data = [('2020-05-30','Headphone'),('2020-06-01','Pencil'),('2020-06-02','Mask'),('2020-05-30','Basketball'),('2020-06-01','Book'),('2020-06-02','Mask'),('2020-05-30','T-Shirt')]  
<b/>
columns = ["sell_date",'product']  

df= spark.createDataFrame(data,schema = columns)  
df.show()  

aggdf = df.groupby("sell_date").agg(  
                                 collect_list("product").alias("product"),  
                                 count("product").alias("null_sell")  
)

aggdf.show()
