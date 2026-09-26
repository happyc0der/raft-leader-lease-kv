class metadump():
    def __init__(self,file_path):
        self.file_path=file_path
    
    def read_metadata(self):
        metadata = {}
        with open(self.file_path, 'r', encoding='utf-8') as file:
            for line in file:
                if '=' in line:
                    key, value = line.strip().split('=')
                    metadata[key.strip()] = value.strip()
        return metadata

    def update_metadata(self, key, value):
        with open(self.file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        with open(self.file_path, 'w', encoding='utf-8') as file:
            for line in lines:
                if line.startswith(key):
                    file.write(f"{key} = {value}\n")
                else:
                    file.write(line)
    
    def write_blank_metadata_file(self):
        with open(self.file_path, 'w', encoding='utf-8') as file:
            file.write("# Metadata File\n\n")
            file.write("# Variable values\n")
            file.write("commitLength = NA\n")
            file.write("Term = NA\n")
            file.write("NodeID = NA\n\n")