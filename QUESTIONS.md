## 📖 Problem Statement
This project aims to analyze a **retail transaction dataset** using **SQL-based queries** in **Hive, PySpark, and SQLite**. The objective is to extract meaningful insights from **customer purchase behavior, aisle trends, product recommendations, and order timing patterns**.

## 🛠 Key Business Questions Addressed
The following **7 business-critical questions** will be answered using SQL queries:

### 1️⃣ Identify the Most Popular Aisles
**📌 Question:**  
➡ **Which aisles have the highest number of products ordered?**  
Understanding which aisles drive the most orders helps optimize **product placement, inventory stocking, and aisle organization**.

**🔍 Query Objective:**  
- Find the **top 3 aisles** with the most product orders.  
- Prioritize stocking and promotions in high-performing aisles.

---

### 2️⃣ Measure Product Distribution in Aisles
**📌 Question:**  
➡ **What is the average (mean) and variance of the number of products available per aisle?**  
This helps analyze **aisle congestion, stocking strategies, and product diversity**.

**🔍 Query Objective:**  
- Calculate **mean** and **variance** of product counts per aisle.  
- Identify aisles with **too many** or **too few products**.

---

### 3️⃣ Customer Buying Patterns Across Aisles
**📌 Question:**  
➡ **Which aisle products are frequently purchased together with products from the ‘Specialty Cheeses’ aisle?**  
Understanding cross-aisle purchases helps in **bundle offers, promotional placements, and personalized recommendations**.

**🔍 Query Objective:**  
- Find **which products are often bought with ‘Specialty Cheeses’**.  
- Optimize aisle placement for complementary product sales.

---

### 4️⃣ Should Frozen Products Be Placed Near Bakery Products?
**📌 Question:**  
➡ **Do customer orders show a strong correlation between frozen and bakery products?**  
Retailers need to know if placing frozen items **next to bakery** increases sales.

**🔍 Query Objective:**  
- Analyze **order co-occurrence** between **Frozen** and **Bakery** products.  
- Validate or challenge sales department recommendations.

---

### 5️⃣ Most Reordered Products
**📌 Question:**  
➡ **Which products are the most frequently reordered by customers?**  
This helps in **customer loyalty programs, restocking strategies, and subscription models**.

**🔍 Query Objective:**  
- Identify the **top 5 most reordered products**.  
- Optimize **inventory forecasting** and **promotions**.

---

### 6️⃣ Best Products for Customer Retargeting Campaign
**📌 Question:**  
➡ **Which products should be discounted in a retargeting campaign to bring customers back?**  
Products that are **rarely reordered** should be targeted with **discounts or incentives** to encourage repeat purchases.

**🔍 Query Objective:**  
- Identify **products with the least repeat purchases**.  
- Recommend **discount offers** to boost customer retention.

---

### 7️⃣ How Do Orders Vary by Time of Day?
**📌 Question:**  
➡ **Are certain product categories more popular in the morning vs. evening?**  
This helps in **staff scheduling, stocking schedules, and targeted promotions**.

**🔍 Query Objective:**  
- Analyze **order trends** based on **time of day**.  
- Find **morning vs. evening peak departments**.

---

## 🛠 How This Data Helps Retailers
✔ **Optimized product placement** based on aisle performance.  
✔ **Better customer experience** with relevant bundle offers.  
✔ **Data-driven marketing campaigns** for retargeting customers.  
✔ **Efficient staff & inventory management** based on time-based order trends.  

---

## 💡 Next Steps
🔹 **Run the SQL queries** inside the `queries/` folder.  
🔹 **View query results** in the `screenshots/` folder.  
🔹 **Extend the project** with **ML-based predictive analytics**.

---

## 📩 Contributions
If you have ideas for more queries, feel free to:
- Fork this repo & submit PRs.
- Open issues for discussions.

🚀 Let’s explore data-driven retail insights together!

---

## 📜 License
This project is licensed under **MIT License**.

---

## 🌟 Star This Repository
If you find this useful, consider giving it a ⭐ on GitHub!
