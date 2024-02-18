
 // 1.1. Read the data and examine the collections. 

 //Answer:
 use('dbAmongUs')
 db.Among_Us_data.find().limit(5)

//1.2. Display data for matches where the "game" field equals "3".

//Answer:
 use('dbAmongUs')
 db.Among_Us_data.find({"game": "3"})

//2. In this subtask, you are expected to create a new 
//collection with only the document relating to game 3.

//2.1. Show the Game Feed data specifically for game 3 in the newly created collection.  

  //Answer:
use('dbAmongUs')
db.Among_Us_data.aggregate([
    { $match: {"game": "3"} },
    { $out: "game3Collection" }
  ])
use('dbAmongUs')
db.game3Collection.find({}, { "Game_Feed": 1 })

//2.2. Show the most recent event that occurred in game 3.

//Answer:
use('dbAmongUs')
db.game3Collection.aggregate([
    { $project: {
      "Most_Recent_Event": { $arrayElemAt: ["$Game_Feed", -1] }
    }}
  ])
  
  
  
//2.3. Determine the winner of game 3 - imposters or crew. 

//Answer:
use('dbAmongUs')
db.game3Collection.aggregate([
    { $unwind: "$Game_Feed" },
    { $sort: { "Game_Feed.Event": -1 } },
    { $limit: 1 },
    { $project: { "Outcome": "$Game_Feed.Action" } }
  ])
  

//2.4. Identify the player who chose the black color in game 3 and whether they were a crew member or imposter. 

//Answer:
use('dbAmongUs')
db.game3Collection.find({"player_data.Color": "Black"}, {"player_data.$": 1})

//2.5. Count the number of voting events that took place in game 3. 

//Answer:
use('dbAmongUs')
db.game3Collection.aggregate({$project:{cnt:{$size:'$voting_data.Vote_Event'}}})


//2.6. If you were redesigning this database for easier querying,
// describe the changes you would make and explain your reasoning. 


//Answer:
//Focused Query: The query directly searches for a player in a specific game with the specified color. It's more straightforward and focused on the player data.

//Performance: By having a separate collection for players, indexed on gameId and color, the database can quickly retrieve relevant player data without searching through nested arrays or subdocuments.

//Scalability and Flexibility: This structure allows the database to efficiently manage a large number of players and games. It also provides flexibility for other types of player-related queries.

//Data Integrity and Clarity: Separating player data into its own collection can help maintain data integrity and makes understanding the database schema easier.

//Adaptability for Other Queries: With a dedicated player collection, queries about player behavior, statistics, and patterns across multiple games become more straightforward.

//By restructuring the data in this way,
// we can make the database schema more efficient for common queries and analyses, enhancing both performance and usability.

//IN COMMON
//Normalized Data Structure: I will split complex documents into separate collections for games, players, and events, linking them with IDs. This will make my queries simpler and improve overall performance.

//Indexing: I'll create indexes on frequently queried fields such as game IDs and player IDs to speed up search operations significantly.

//Consistent Field Types: I would ensure that all fields have consistent data types across documents to prevent confusion and errors during querying.

//Denormalization for Accessibility: For data that is often accessed together, like some player information in game documents, I'll use a degree of denormalization to facilitate faster access.

//Timestamps for Events: will add timestamps to each event in the game feed, enabling easier analysis of the sequence and timing of events.

//Balanced Use of Subdocuments and Arrays: will sensibly use subdocuments and arrays for hierarchical or grouped data,
 //and ensuring the efficiency of queries.

//Schema Design Based on Query Patterns: would design the schema to align with common query patterns, enhancing the user experience when querying data related to individual games or players.

//Data Validation Rules: I'll implement rules for data validation to maintain data integrity, like enforcing the presence of specific fields and setting acceptable ranges for values.

//Aggregation Framework Optimization: I will structure the schema to optimize the use of MongoDB's aggregation framework, which is crucial for complex queries and data analysis.



//3.1. Calculate the total number of events recorded in this collection across all games. 

//Answer:
use('dbAmongUs')
db.Among_Us_data.aggregate([
    { $unwind: "$Game_Feed" },
    { $count: "Total_Events" }
  ])
  

//3.2. Compare the crew's wins to the impostors' wins and provide the counts. 

//Answer:
use('dbAmongUs')
db.Among_Us_data.aggregate([
    { $project: { "Outcome": { $arrayElemAt: ["$Game_Feed.Action", -1] } } },
    { $group: {
      _id: "$Outcome",
      Count: { $sum: 1 }
    }}
  ])


  use('dbAmongUs')
  db.Among_Us_data.find({'Game_Feed.Game Feed':{$regex:'Crew Win'}}).count()

  
  use('dbAmongUs')
  db.Among_Us_data.find({'Game_Feed.Game Feed':{$regex:'Impostor Win'}}).count()



//3.3. List the maps played and the total number of games on each map. 

//Answer:
use('dbAmongUs')
db.Among_Us_data.aggregate([
    { $unwind: "$Game_Feed" },
    { "$match" : {"Game_Feed.Event":1}},
{ "$group" : {"_id": "$Game_Feed.Map","count": { "$sum":1}}}
])
  

//3.4. Determine the total instances of crew members skipping a vote across all games. 

//Answer:
use('dbAmongUs')
db.Among_Us_data.aggregate([
  { $unwind: "$Game_Feed" },
  { $match: { "Game_Feed.Game Feed": /skips voting/i } },
  { $count: "SkippedVotes" }
])




//3.5. Calculate the total occurrences of crew members voting against imposters across all matches. 

//Answer:
use('dbAmongUs')
db.Among_Us_data.aggregate([
  { $unwind: "$Game_Feed" },
  { $match: { "Game_Feed.Day 1 vote": /impostor voted off/i } },
  { $count: "VotesAgainstImpostors" }
])



//3.6. Share your opinion on whether the game is more or less challenging for impostors, supported by insights from the data. 

//Answer:
//Based on the data
//High Impostor Ejection Rate: With 893 instances of impostors being voted off, 
//the data suggests that impostors face a considerable challenge in evading detection and maintaining their cover.

//Crew Diligence: The frequency of impostor ejections points to crew members being proactive and effective in discussions 
//and voting, thereby increasing the difficulty for impostors to survive votes.

//Lack of Complete Win/Loss Data: Without specific win/loss ratios for impostors versus crewmates,
// the data is incomplete in knowing a full picture of the game's difficulty balance.

//Implications of Skip Votes: There were 693 instances of skipped votes, 
//which may indicate a level of uncertainty during voting discussions, 
//potentially providing a slight advantage to impostors.

//Data-Driven Inference: Although direct win/loss metrics are missing,
// the high number of impostor ejections allows us to infer that playing as an impostor 
//could be challenging due to the effectiveness of crew strategy during voting sessions.




//4.1. Find the count of unique players in the dataset. 

//Answer:
use('dbAmongUs')
db.Among_Us_data.aggregate([
    { $unwind: "$player_data" },
    { $group: { _id: "$player_data.name" } },
    { $group: { _id: null, count: { $sum: 1 } } }
  ])
  
//4.2. Identify the player considered the best crew member. 


//Answer:
use('dbAmongUs') 
db.Among_Us_data.aggregate([
    {"$unwind": "$voting_data"},
    {"$match": {"voting_data.Vote": {"$regex": "Impostor voted off"}} },
    {"$group": {"_id": "$voting_data.name", "Count": {"$sum": 1}}},
    {"$sort": {"Count": -1}},
    {"$limit": 1} // Add the $limit stage here
])




//4.3. Identify the player regarded as the least effective crew member. 

//Answer:
use('dbAmongUs') 
    db.Among_Us_data.aggregate([
        {"$unwind": "$voting_data"},
        {"$match": {"voting_data.Vote": {"$regex": "Crew voted off"}} },
        {"$group": {"_id": "$voting_data.name", "Count": {"$sum": 1}}},
        {"$sort": {"Count": -1}},
        {"$limit": 1} // Add the $limit stage here
    ])
    

//4.4. Compute the win percentage for each player. (Optional)

//Answer:
use('dbAmongUs');
db.Among_Us_data.aggregate([
    // Unwind the nested arrays to work with individual elements
    { $unwind: "$player_data" },
    { $unwind: "$Game_Feed" },

    // Filter only the games that have an outcome (End of the game)
    { $match: { "Game_Feed.Outcome": { $regex: "End" } } },

    // Project the necessary fields
    { $project: { 
        "player_name": "$player_data.name",
        "role": { $substr: ["$player_data.Role", 1, 4] },
        "result": { $substr: ["$Game_Feed.Action", 0, 4] }
    }},

    // Determine if the player won the game
    { $project: { 
        "player_name": 1,
        "win": { $cond: [{ $eq: ["$result", "$role"] }, 1, 0] }
    }},

    // Group by player and calculate total games and wins
    { $group: {
        _id: "$player_name",
        total_games: { $sum: 1 },
        total_wins: { $sum: "$win" }
    }},

    // Calculate win percentage
    { $project: {
        "_id": 0,
        "player_name": "$_id",
        "win_percentage": { 
            $round: [{ 
                $multiply: [{ 
                    $divide: ["$total_wins", "$total_games"] 
                }, 100] 
            }, 2]
        }
    }}
]);

//4.5. Determine the color preferences chosen by all players. (Optional)

//Answer:
use('dbAmongUs')
db.Among_Us_data.aggregate([
    { $unwind: "$player_data" },
    { $group: { _id: "$player_data.name", colors: { $addToSet: "$player_data.Color" } } }
  ])
  





//5 . Create an export from MongoDB in the form given below as player name ,
// games won as imposter, games won as crew, win percentage(overall) voted against imposter, 
//voted against crew members , color preference, voting rate

// creating table for color
use('dbAmongUs')
db.Among_Us_data.aggregate([
    { $unwind: "$player_data"},
    { $group: { 
        _id: { Player: "$player_data.name", Color: "$player_data.Color"}, 
        Total: { $sum: 1 }}},
    { $group: { 
        _id : "$_id.Player",
        Colors: { $push: { Color: "$_id.Color", Count:"$Total"}} }},
    { $unwind: "$Colors" },
    { $sort: { "Colors.Count": -1 } },
    { $group: { _id: "$_id", details: { $push : "$Colors" }}},
    { $project: { _id:1 , colors: { $first: "$details"}}},
    { $project: { _id:1 , Preference: "$colors.Color" }},
    { $out: "Color"}])


// creating table for win
    use('dbAmongUs')
db.Among_Us_data.aggregate([
    { $unwind: "$player_data"},
    { $unwind: "$Game_Feed"},
    { $match: { "Game_Feed.Outcome": { $regex:"End"}} },
    { $project: { 
        "player_data.name": 1,
        Role: { $substr: [ "$player_data.Role", 1, 4 ]} ,
        Result: { $substr: [ "$Game_Feed.Action", 0, 4 ]}}},
    { $project: { 
        "player_data.name": 1,
        Win: { $cond: [{ $eq:["$Result","$Role"]}, 1, 0]},
        Crew_Win: { $cond: [{ $and:[{ $eq:["$Result","Crew"]},{ $eq:["$Role","Crew"]}]},1,0]},
        Imposter_Win: { $cond: [{ $and:[{ $eq:["$Result","Impo"]},{ $eq:["$Role","Impo"]}]},1,0]}}},
    { $group: {
        _id: "$player_data.name",
        Played: { $sum:1 },
        Wins: { $sum: "$Win" },
        Win_As_Crew: { $sum: "$Crew_Win" },
        Win_As_Imposter: { $sum: "$Imposter_Win" }}},
    { $project: {
        _id: 1,
        Win_As_Crew: 1,
        Win_As_Imposter: 1,
        Win_Rate:{$multiply:[{ $divide:["$Wins", "$Played"]}, 100]}}},
    { $out: "Win"}])


    // creating table for 'Voting'
use('dbAmongUs')
db.Among_Us_data.aggregate([
    { $unwind: "$voting_data"},
    { $match: { "voting_data.Is_alive": { $regex:"Yes"}} },
    { $project: { 
        "voting_data.name": 1,
        "AgainstCrew":{ $cond:[{ $regexMatch:{ "input":"$voting_data.Vote", "regex": "Crew"}},1,0]},
        "AgainstImpo":{ $cond:[{ $regexMatch:{ "input":"$voting_data.Vote", "regex": "Impostor"}},1,0]} }},
    { $group: { 
        _id: "$voting_data.name",
        Voted_Against_Crew: { $sum : "$AgainstCrew" },
        Voted_Against_Impo: { $sum : "$AgainstImpo" },
        Voting_Opportunities: { $sum: 1} }},
    { $project: { 
        _id: 1,
        Voted_Against_Crew: 1,
        Voted_Against_Impo: 1,
        Voting_rate:
        { $multiply:[{$divide:[{$add:["$Voted_Against_Crew","$Voted_Against_Impo"]},"$Voting_Opportunities"]} ,100]} }},
    { $out: "Voting"}])

    // merging all the tables together
    use('dbAmongUs')
    db.Win.aggregate([
        { $lookup: {
            from: "Voting", 
            localField: "_id", 
            foreignField: "_id", 
            as: "fromVoting" }},
        { $unwind: { path: "$fromVoting", preserveNullAndEmptyArrays: true }},
        { $merge: {
            into: "temp", 
            whenMatched: "merge", 
            whenNotMatched: "insert" }}])
    use('dbAmongUs')
    db.Color.aggregate([
        { $lookup: {
            from: "temp", 
            localField: "_id", 
            foreignField: "_id", 
            as: "fromWin_Voting" }},
        { $unwind: { path: "$fromWin_Voting", preserveNullAndEmptyArrays: true }},
        { $merge: {
            into: "Final_data", 
            whenMatched: "merge", 
            whenNotMatched: "insert" }}])

            ///and then rename the fields
            use('dbAmongUs')
            db.Final_data.aggregate([
                { $project: {
                    _id:0,
                    "Player name": "$_id",
                    "Games won as imposter": "$fromWin_Voting.Win_As_Imposter",
                    "Games won as crew": "$fromWin_Voting.Win_As_Crew",
                    "Win percentage(overall)": "$fromWin_Voting.Win_Rate", 
                    "Voted Against Imposter": "$fromWin_Voting.fromVoting.Voted_Against_Impo",
                    "Voted against Crew members": "$fromWin_Voting.fromVoting.Voted_Against_Crew",  
                    "Color preference": "$Preference", 
                    "Voting rate": "$fromWin_Voting.fromVoting.Voting_rate" }},
                { $out: "AmongUs_CSVExport"}]) 


// Export code //
mongoexport --db=dbAmongUs --collection=AmongUs_CSVExport --type=csv --fields="Player name","Games won as imposter","Games won as crew","Win percentage(overall)","Voted Against Imposter","Voted against Crew members","Color preference","Voting rate" --out="C:\Users\ramph\Downloads\Advanced SQL Project\Archana_AmongUs_CSVExport.csv"




//6.In tasks 4 and 5, you create individual player statistics. 
//Discuss additional statistics that might be worth exploring (aside from those already calculated) 
//while selecting players. (There's no requirement to write code for them.)



//Answers:
//Task Efficiency: We can Calculate how efficiently a player completes tasks as a crew member,
// indicating their diligence in fulfilling crew duties.

//Survival Skill:By Determining the percentage of games a player survives until the end, 
//showcasing their ability to stay alive.

//Impostor win Rate: Calculating the percentage of impostor games won by the player,
// reflecting their effectiveness in deceiving the crew.

////Crew Victory Rate: Calculating the percentage of crew member games won by the player, 
//demonstrating their skill in identifying impostors and completing tasks.

//Meeting Engagement: We can Count how often a player calls emergency meetings, 
//revealing their involvement in game discussions.

//Teamwork: Analyzing if a player prefers working alone or collaborates with others to achieve game goals.

//Vote Patterns: Studying a player's voting behavior, such as who they vote for and their preferences for impostors or crew members.

//Time Played: Calculating the total time a player spends in the game, indicating their experience level.

//Communication: Can analyze in-game chat patterns and communication frequency among players.

//Consistency: We can Assess a player's performance consistency across multiple games.

//Preferred Roles: To Identify a player's preferred role (crew or impostor) and 
//calculate their performance and win rates based on their preference.


//Tasks Completed as Impostor: Calculating the number of tasks completed by the player while playing as an impostor.

//Task Interruption as Impostor: We can Count how many times a player disrupts crew members 
//while they are completing tasks as an impostor.


//Map Proficiency: Analyzing a player's performance on different maps to identify strengths and weaknesses.

//Social Interaction: Can perform a study about player's interactions with others, including alliances, conflicts, and cooperation.




