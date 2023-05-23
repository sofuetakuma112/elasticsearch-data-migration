import os


if __name__ == "__main__":
    some_list = [1, 2, 3, 4, 5]

    try:
        for index, item in enumerate(some_list):
            if item == 3:
                raise ValueError("エラーが発生しました")
                
            print(f"index: {index}, item: {item}")

    except ValueError as e:
        print("エラーが発生しました。index:", index)
        print("エラーメッセージ:", str(e))
