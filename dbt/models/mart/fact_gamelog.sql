with game_logs as (
    select
        *
    from {{ source('stage', 'game_logs') }}
),
final as (
    select
        game_date
        ,upper(trim(game_num)) as game_num
        ,day_of_week
        ,{{ dbt_utils.surrogate_key(['vis_team_name', 'vis_team_league']) }} as vis_team_key
        ,{{ dbt_utils.surrogate_key(['home_team_name', 'home_team_league']) }} as home_team_key
        ,case when upper(trim(park_id)) in ('OMA01', 'LON01') 
            then '00000' 
            else coalesce(upper(trim(park_id)), '00000')
         end as park_key
        ,coalesce(nullif(upper(trim(vis_team_mgr_id)), ''), '00000000') as vis_team_mgr_id
        ,coalesce(nullif(upper(trim(home_team_mgr_id)), ''), '00000000') as home_team_mgr_id
        ,coalesce(nullif(upper(trim(winning_pitcher_id)), ''), '00000000') as winning_pitcher_id
        ,coalesce(nullif(upper(trim(losing_pitcher_id)), ''), '00000000') as losing_pitcher_id
        ,coalesce(nullif(upper(trim(saving_pitcher_id)), ''), '00000000') as saving_pitcher_id
        ,case
            when upper(trim(day_night_ind)) = 'D' THEN 'Day'
            when upper(trim(day_night_ind)) = 'N' THEN 'Night'
            when upper(trim(day_night_ind)) = '' THEN 'Unknown'
         end AS day_night_ind
        ,attendance
        ,vis_team_score
        ,home_team_score
        ,game_len_mins
        ,vis_team_at_bats        
        ,vis_team_hits           
        ,vis_team_doubles        
        ,vis_team_triples        
        ,vis_team_hr             
        ,vis_team_rbi            
        ,vis_team_sachits        
        ,vis_team_sacflies       
        ,vis_team_hbp            
        ,vis_team_bb             
        ,vis_team_ibb            
        ,vis_team_strikeouts     
        ,vis_team_stolen_bases   
        ,vis_team_caught_stealing
        ,vis_team_gidp           
        ,vis_team_awarded_first_catcher_interference
        ,vis_team_lob                                 
        ,vis_team_pitchers_used                       
        ,vis_team_indiv_earned_runs                   
        ,vis_team_team_earned_runs                    
        ,vis_team_wild_pitches                        
        ,vis_team_balks                               
        ,vis_team_putouts                             
        ,vis_team_assists                             
        ,vis_team_errors                              
        ,vis_team_passed_balls                        
        ,vis_team_double_plays                        
        ,vis_team_triple_plays                        
        ,home_team_at_bats                            
        ,home_team_hits                               
        ,home_team_doubles                            
        ,home_team_triples                            
        ,home_team_hr                                 
        ,home_team_rbi                                
        ,home_team_sachits                            
        ,home_team_sacflies                           
        ,home_team_hbp                                
        ,home_team_bb                                 
        ,home_team_ibb                                
        ,home_team_strikeouts                         
        ,home_team_stolen_bases                       
        ,home_team_caught_stealing                    
        ,home_team_gidp                               
        ,home_team_awarded_first_catcher_interference 
        ,home_team_lob                                
        ,home_team_pitchers_used                      
        ,home_team_indiv_earned_runs                  
        ,home_team_team_earned_runs                   
        ,home_team_wild_pitches                       
        ,home_team_balks                              
        ,home_team_putouts                            
        ,home_team_assists                            
        ,home_team_errors                             
        ,home_team_passed_balls                       
        ,home_team_double_plays                       
        ,home_team_triple_plays                     
    from game_logs
    where date_part('year', game_date) >= 1900
)
select
    *
from final