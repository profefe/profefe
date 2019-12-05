## S3 profile storage

### Schema

each profile is stored at:

/service/profile_type/created_at_time/label1,label2/id
contents:
{
"profile_id": "id",
"service": "service1",
"type": "cpu",
"instance_id": "id",
"labels": "label1",
"created_at": 2019
"s3": "/profiles/id.gz",
}

/service/profile_type/created_at_time/label1,label2/instance_id

/profiles/id.gz

FindProfiles() {
s3 --prefix /service/profile_type/created_at_time
for each object check if labels are in labels
until limit download id meta data
or until time > CreatedAtMax
}

FindProfilesID() {
s3 --prefix /service/profile_type/created_at_time
for each object check if labels are in labels
return id of the path
(/service/profile_type/created_at_time/label1,label2/id)
}

ListProfiles(){
for each id {
download /profiles/.id.gz
}
}

/hour/minute/label1=value/services/ids
/hour/minute/label2=value/services/ids
/hour/minute/label3=value/services/ids
/hour/minute/label4=value/services/ids

--prefix /hour/minute/service/type/label2
--start-after label2

### Required IAM permissions
