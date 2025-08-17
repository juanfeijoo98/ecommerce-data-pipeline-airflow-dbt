import pandas as pd 
import random

## Creamos un archivo CSV con datos aleatorios

def generate_datos(n=1000):
    data = {
        "order_id": [i for i in range(1, n+1)],
        "product" : [random.choice(["Zapatos","Camiseta","Pantalon", "Gorra"]) for _ in range(n)],
        "quantity": [random.randint(1, 5) for _ in range(n)],
        "price": [round(random.uniform(10, 100), 2) for _ in range(n)],
    }
    df = pd.DataFrame(data)
    return df

if __name__ == "__main__":
    df = generate_datos(500)
    output_path = "data_seed/ventas_2024.csv"
    df.to_csv(output_path, index = False)
    print(f"El archivo se genero correctamente {output_path}")
    