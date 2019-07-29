import glob2
import pandas as pd

#####################txt to csv ########################
def text_to_csv():
    con_filenames = glob2.glob('E:/MA/MA_dataset/Preproccessed/Dementia bank/dementia/*.txt')  # list of all .txt files in the directory
    for i in con_filenames:
        df = pd.read_fwf(i)
        p=i.split('E:/MA/MA_dataset/Preproccessed/Dementia bank/dementia')[1]
        p=p.split('.txt')[0]
        df.to_csv('E:/MA/MA_dataset/Preproccessed/dementia_csv'+p+'.csv',index=None,header=False)

    #########################merging rows into one row ########################
def merge_row_in_one_row():
    con_filenames = glob2.glob('E:/MA/MA_dataset/Preproccessed/dementia_csv/*.csv')
    for i in con_filenames:
        p=i.split("E:/MA/MA_dataset/Preproccessed/dementia_csv")[1]
        fIn = open(i, "r",encoding="utf8")
        fOut = open("E:/MA/MA_dataset/Preproccessed/Dementia bank/dementia_csv_merge_data"+p, "w",encoding="utf8")
        fOut.write(",".join([line for line in fIn]).replace("/n",""))
        fIn.close()
        fOut.close()

###########merging columns into one#################
def merge_col_one_col():
    con_filenames = glob2.glob('E:/MA/MA_dataset/Preproccessed/Dementia bank/dementia_csv_merge_data/*.csv')
    for i in con_filenames:
        p=i.split("E:/MA/MA_dataset/Preproccessed/Dementia bank/dementia_csv_merge_data")[1]
        df=pd.read_csv(i,header=None)
        df['message'] = df[df.columns[0:]].apply(
            lambda x: ','.join(x.dropna().astype(str)),
            axis=1)
        df.drop(df.columns.difference(['message']), 1, inplace=True)
        # df.insert ( 0 , "Label" , ["control"] , True )
        df.to_csv ('E:/MA/MA_dataset/Preproccessed/Dementia bank/dementia_csv_merge_data_columns'+ p , index=None,header=None )

##########making one file of multiple files #####################
def making_one_file_of_multiple_file():
    import glob2
    con_filenames = glob2.glob('E:/MA/MA_dataset/Preproccessed/Dementia bank/dementia_csv_merge_data_columns/*.csv')  # list of all .txt files in the directory
    with open('dementia_output_files@.csv','wb') as newf:
        for filename in con_filenames:
            with open(filename,'rb') as hf:
                newf.write(hf.read())
                #newf.write (b'/n')

##########adding label column and naming second column s message ################
def add_label_and_naming_rest_col():
    import pandas as pd
    df=pd.read_csv('E:/MA/dementia_output_files@.csv',header=None)
    df.columns=['message']
    df.insert ( 0 , "Label" , "dementia" , True )
    df.to_csv('dementia_output_files@.csv',index=None)
    print(df.columns)

def label_count():
    df=pd.read_csv('E:/MA/final_output_file@.csv')
    print(df['Label'].value_counts())

####Second Time without merging rows making dementia output file #######
def second_time_merge_rows_file():
    import glob2
    con_filenames = glob2.glob('E:\MA\MA_dataset\Preproccessed\dementia_csv/*.csv')  # list of all .txt files in the directory
    with open('dementia_output_files@@.csv','wb') as newf:
        for filename in con_filenames:
            with open(filename,'rb') as hf:
                newf.write(hf.read())
                # newf.write (b'/n')

######Same thing with control output file ##################
def second_time_control_file():
    import glob2
    con_filenames = glob2.glob('E:\MA\MA_dataset\Preproccessed\Dementia bank\control_csv/*.csv')  # list of all .txt files in the directory
    with open('control_output_files@@.csv','wb') as newf:
        for filename in con_filenames:
            with open(filename,'rb') as hf:
                newf.write(hf.read())

#############adding label column and naming second column s message ################
def add_label_second_time():
    import pandas as pd
    df=pd.read_csv('E:\MA\dementia_output_files@@.csv',header=None, error_bad_lines=False)
    df.columns=['message']
    df.insert ( 0 , "Label" , "dementia" , True )
    df.to_csv('dementia_output_files@@.csv',index=None)
    print(df.columns)

############adding 5 column into one of control ########################
def add_five_col_one_control():
    import pandas as pd
    df=pd.read_csv('E:\MA\control_output_files@@.csv',header=None, error_bad_lines=False)
    df['message'] = df[df.columns[0:]].apply(
        lambda x: ','.join(x.dropna().astype(str)),
        axis=1)
    df.drop(df.columns.difference(['message']), 1, inplace=True)
    df.insert ( 0 , "Label" , "control" , True )
    df.to_csv ('control_output_files@@.csv',index=None)
    print(df.columns)

if __name__ == '__main__':
    text_to_csv()
    merge_row_in_one_row()
    merge_col_one_col()
    making_one_file_of_multiple_file()
    add_label_and_naming_rest_col()
    label_count()
    second_time_merge_rows_file()
    second_time_control_file()
    add_label_second_time()
    add_five_col_one_control()
    add_five_col_one_control()



