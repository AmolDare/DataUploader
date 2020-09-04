import yaml
import datetime
import pandas as pd 
from sqlalchemy import create_engine
from unique_id import get_unique_id
import time
import os

start = time.time()


path = 'C:\\Users\\Amol\\Downloads\\odis_male'
#path ='C:\\Users\\Amol\\Desktop\\test'

files = []
# r=root, d=directories, f = files
for r, d, f in os.walk(path):
    for file in f:
        if '.yaml' in file:
            files.append(os.path.join(r, file))

for f in files:
    with open(f, 'r') as stream:
        try:
            dict=yaml.safe_load(stream)
            base=os.path.basename(f)
            id=os.path.splitext(base)[0]
        except yaml.YAMLError as exc:
            print(exc)

    info=dict["info"]


    df=pd.DataFrame()
    df2=pd.DataFrame()

    df['date']=info.get("dates")
    df['match_id']=id

    df['city']=info.get("city")
    df['gender']=info.get("gender")
    df['match_type']=info.get("match_type")

    if info.get("outcome") is not None:
        
        if info.get("outcome").get("winner") is not None:
            df['winner']=info.get("outcome").get("winner")
            df['isDraw'] =False
        else:
            df['winner']=None
            df['isDraw'] =True

        if info.get("outcome").get("by") is not None:
            if info.get("outcome").get("by").get('runs') is not None:
                df['byRuns']=info.get("outcome").get("by").get('runs')
                df['byWicket']=None
            elif info.get("outcome").get("by").get('wickets') is not None:
                df['byWicket']=info.get("outcome").get("by").get('wickets')
                df['byRuns']=None  

        else:
            df['byWicket']=None
            df['byRuns']=None

            
    
            
            
 
        
      
        
        if info.get("outcome").get("method") is not None:
            df['method']=info.get("outcome").get("method")
        else:
            df['method']=None

    else:
        df['byWicket']=None
        df['byRuns']=None
        df['isDraw'] =True
        df['method']=None

    df['overs']= info.get("overs")
    if info.get("player_of_match") is not None:
        for player in  info.get("player_of_match"):
            player=" "+player
        df['player_of_match']= player
    else:
       df['player_of_match']= info.get("player_of_match")   
    df['team1']=info.get("teams")[0]
    df['team2']=info.get("teams")[1]
    df['tossDecision']=info.get("toss").get("decision")
    df['tossWinner']=info.get("toss").get("winner")
    df['umpire1']=info.get("umpires")[0]
    df['umpire2']=info.get("umpires")[1]
    df['venue']=info.get("venue")
    df['udf1']=None
    df['udf2']=None
    df['udf3']=None
    df['udf4']=None
    df['udf5']=None

    team=[]
    deliveries=[]
    batsman= []
    bowler= []
    non_striker=[]   
    runs_batsman = []  
    runs_total=[]
    extras=[]
    match_inning=[]
    noballs=[]
    wides=[]
    legbyes= []
    wicket=[]
    wicket_kind=[]
    wicket_fielders=[]
    player_out=[]
    match_id1=[]

    innings=dict["innings"]

    for inning in innings:
        for key1, value1 in inning.items():
            if value1 is not None:
                if value1["deliveries"] is not None:
                    for delivery in value1["deliveries"]:
                        for key, value in delivery.items():
                            match_inning.append(key1)
                            team.append(value1.get("team"))
                            deliveries.append(key)
                            match_id1.append(id)
                            batsman.append(value['batsman'])
                            bowler.append(value['bowler'])
                            non_striker.append(value['non_striker']) 
                            runs_batsman.append(value['runs'].get('batsman'))  
                            runs_total.append(value['runs'].get('total'))
                            extras.append(value['runs'].get('extras'))
                            if value.get('extras' )is not None:
                                if value['extras'].get('wides') is not None:
                                    wides.append(value['extras'].get('wides'))
                                else:
                                    wides.append(0)
                    
                                if value['extras'].get('noballs') is not None:
                                    noballs.append(value['extras'].get('noballs'))
                                else:
                                    noballs.append(0)  

                                if value['extras'].get('legbyes') is not None:
                                    legbyes.append(value['extras'].get('legbyes'))
                                else:
                                    legbyes.append(0)    

                            else:
                                wides.append(0)
                                noballs.append(0)
                                legbyes.append(0)


                            if value.get('wicket') is not None:
                                wicket.append(True)
                                wicket_kind.append(value['wicket'].get('kind'))
                                player_out.append(value['wicket'].get('player_out'))
                                if value['wicket'].get('fielders') is not None:
                                    for fielder in value['wicket'].get('fielders'):
                                        fielder=" "+fielder
                                    wicket_fielders.append(fielder)
                                else:
                                    wicket_fielders.append(None)

                            else:
                                wicket.append(False)
                                wicket_kind.append(None)
                                player_out.append(None)
                                wicket_fielders.append(None)

    df2['match_id'] =match_id1
    df2['match_inning']=match_inning
    df2['deliveries']=deliveries
    df2 ['team']=team
    df2['batsman']= batsman
    df2['bowler']= bowler
    df2['non_striker']=non_striker  
    df2['runs_batsman'] = runs_batsman
    df2['runs_total']=runs_total
    df2['extras']=runs_total
    df2['noballs']=noballs
    df2['wides']=wides
    df2['legbyes']= legbyes
    df2['wicket']=wicket
    df2['wicket_kind']=wicket_kind
    df2['wicket_fielders']=wicket_fielders
    df2['player_out']=player_out

    #Write into database
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="admin",
                               db="daja"),pool_pre_ping=True)

    try:
        df.to_sql('match_summary', con = engine, if_exists = 'append', chunksize = 1000,index=False)
    except Exception as exc:  
        print(exc)

    try:
        df2.to_sql('match_detail', con = engine, if_exists = 'append', chunksize = 1000,index=False)
    except Exception as exc:  
        print(exc)

# end time
end = time.time()

# total time taken
print(f"Runtime of the program is {end - start}")






    






                




                
                

                
                


            
            


    
        





                    

    



        
            

