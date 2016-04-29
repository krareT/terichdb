
function terarkCreateColl(dbname, collname, schemaFile, opt) {
    var schemaJsonString = cat(schemaFile);
    var schemaJsonObject = JSON.parse(schemaJsonString);
    var dbObj = db.getSiblingDB(dbname);
    var opt2 = {};
    for (var key in opt) {
        opt2[key] = opt[key];
    }
	if (!schemaJsonObject["CheckMongoType"]) {
		 schemaJsonObject["CheckMongoType"] = true;
		 print("WARN: 'CheckMongoType' is false or missing, set it to true, 'mongoType' for critical fields should have been set");
	}
    opt2["storageEngine"] = {
        "TerarkSegDB" : schemaJsonObject
    };
//	print("opt2: " + JSON.stringify(opt2));
    dbObj.createCollection(collname, opt2);

    // let mongodb know the indices
    var indices = schemaJsonObject["TableIndex"];
    for(var i = 0; i < indices.length; ++i) {
        var x = indices[i];
        var xFields = x["fields"];
		var fields = null;
        if (xFields instanceof String || (typeof xFields) === 'string') {
            fields = xFields.split(',');
        }
        else if (xFields instanceof Array || (typeof xFields) === 'array') {
            fields = xFields;
        }
        else {
			print("bad index fields: " + xFields.toString());
			throw("bad index fields: " + xFields.toString());
        }
	//	print("(typeof fields) = " + (typeof fields));
	//	print("(fields instanceof String) = " + (fields instanceof String));
	//	print("(fields instanceof Array) = " + (fields instanceof Array));
        var mongoFields = {};
        for (var j = 0; j < fields.length; ++j) {
		//	print("fields[" + j + "] = " + fields[j]);
            mongoFields[fields[j]] = 1;
        }
        var indexOpt = {};
        if (x.hasOwnProperty("unique")) {
            indexOpt["unique"] = x["unique"];
        } else {
            indexOpt["unique"] = false;
        }
        if (x.hasOwnProperty("name")) {
            indexOpt["name"] = x["name"];
        }
//		print("createIndex: fields=" + fields.toString());
//		print("createIndex(" + JSON.stringify(mongoFields) + ", " + JSON.stringify(indexOpt));
        dbObj[collname].createIndex(mongoFields, indexOpt);
    }
}


