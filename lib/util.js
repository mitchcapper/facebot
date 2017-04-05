var Q = require("q");

function findFriendUserByName(api, name){
    var userID;
    var promise;
    if (/^\d+$/.test(name)){  //if all numbers assume it is the userid
        userID=name;
        promise=Q.nfcall(api.getUserInfo, userID);
    }else{
        promise=Q.nfcall(api.getUserID, name)
                .then(function(data){
                userID = data[0].userID;
                return Q.nfcall(api.getUserInfo, userID);
            });
    }
        promise=promise.then(function(userInfoMap){
            var userInfo = userInfoMap[userID];
            
            if(!userInfo.isFriend) throw new Error("User not your friend, they may not be your top " + name + ", try using '@facebot friends <partial_name>' to get their id or fb vanity name to use");
            
            // The userinfo object doesnt have an id with it, so add it
            userInfo.id = userID;
            return userInfo;
        });
 
    return promise;
}

module.exports = {
    findFriendUserByName: findFriendUserByName
}