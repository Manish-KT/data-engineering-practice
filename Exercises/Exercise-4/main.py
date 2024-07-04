import os
import json


def open_folders_recursively(path, type="json"):
    files_path = []

    for root, dirs, files in os.walk(path):
        print("Current directory:", root)

        for file in files:
            if file.endswith(f".json"):
                # print("File:", os.path.join(root, file))
                files_path.append(os.path.join(root, file))
    
    return files_path


def dictSerach(dict, final_dict):
    if isinstance(dict, type(dict)):
        for key, value in dict.items():
            if isinstance(value, type(dict)):
                dictSerach(value, final_dict)
            else:
                if key not in final_dict:
                    final_dict[key] = []
                final_dict[key].append(value)

def main():
    start_path = os.getcwd()
    print("Starting path:", start_path)
    all_files = open_folders_recursively(start_path, type="json")
    
    final_dict = dict()

    # print("All files:", all_files)
    for path in all_files:
        with open(path, "r") as f:
            file = json.load(f)
            dictSerach(file, final_dict)

    # print("Final dictionary:", final_dict)

    with open("output.txt", "w") as f:
        # columns
        for key in final_dict.keys():
            f.write(key + ",")

        # rows
        f.write("\n")
        for i in range(len(final_dict["name"])):
            for key, value in final_dict.items():
                try:
                    f.write(value[i] + ",")
                except: 
                    f.write(str(value[i]))
            f.write("\n")
        
    os.rename("output.txt", "output.csv")






if __name__ == "__main__":
    main()
