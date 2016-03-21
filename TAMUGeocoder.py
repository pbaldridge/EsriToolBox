import arcpy
import sys
import string
import os
import requests
import time

#get input database from arcmap user input
tablePath = arcpy.GetParameterAsText(0)


arcpy.AddMessage("version: " + sys.version)
#fields_to_add = ["Latitude","Longitude","MatchType","Result"]
fields_to_add = ["Latitude","Longitude","MatchType","Result","Block","Group","Tract","County Fips","CBSA Fips","Micropolitan","MCD Fips","MetDiv","MSA Fips","Place Fips","State Fips","Group"]
fieldList = arcpy.ListFields(tablePath)
fieldName = [f.name for f in fieldList]

for field in fields_to_add:
    if field not in fieldName:
        if field == "Latitude" or field =="Longitude":
            arcpy.AddField_management(tablePath,field,"FLOAT",15,15,"","","NULLABLE","NON_REQUIRED","");
        else:
            arcpy.AddField_management(tablePath,field,"string","","",55,"","NULLABLE","NON_REQUIRED","");


address = arcpy.GetParameterAsText(1)
city = arcpy.GetParameterAsText(2)
state = arcpy.GetParameterAsText(3)
zip = arcpy.GetParameterAsText(4)
apikey = arcpy.GetParameterAsText(5)


#with arcpy.da.UpdateCursor(tablePath, [address,city,state,zip,'Latitude','Longitude','MatchType','Result']) as cursor:
#following section if census data desired
with arcpy.da.UpdateCursor(tablePath, [address,city,state,zip,'Latitude','Longitude','MatchType','Result',"Block","Group","Tract","County Fips","CBSA Fips","Micropolitan","MCD Fips","MetDiv","MSA Fips","Place Fips","State Fips","Group"]) as cursor:

	totalRows = int(arcpy.GetCount_management(tablePath).getOutput(0))
	arcpy.AddWarning("Processing "+str(totalRows)+" total records...")
	#this sets up the progress bar
	arcpy.SetProgressor("step", "Copying shapefiles to geodatabase...",0, totalRows, 1)
	start_time = time.time()
        success = 1
	for row in cursor:
		arcpy.SetProgressorLabel("Loading {0}...".format(row))
		url = "https://geoservices.tamu.edu/Services/Geocode/WebService/GeocoderWebServiceHttpNonParsed_V04_01.aspx?"

		#payload = "streetAddress="+str(row[0])+"&city="+str(row[1])+"&state="+str(row[2])+"&zip="+str(row[3])+"&apikey="+apikey+"&format=csv&census=false&censusYear=2010&notStore=false&version=4.01"
		payload = "streetAddress="+str(row[0])+"&city="+str(row[1])+"&state="+str(row[2])+"&zip="+str(row[3])+"&apikey="+apikey+"&format=csv&census=true&censusYear=2010&notStore=false&version=4.01"

		headers = {
			'accept': "application/x-www-form-urlencoded",
			'content-type': "application/x-www-form-urlencoded",
			'access-control-allow-origin': "*"
			}

		response = requests.request("POST", url, data=payload, headers=headers)
		length = len(response.text)
		if length>0:
			responseList = response.text.split(',')
		else:
			arcpy.AddWarning("There are 0 results")
			arcpy.AddError("This is likely due to an invalid api key")	
			success = 0
			sys.exit(1)			
		responseListOut = []
		row[4] = responseList[3]
		row[5] = responseList[4]
		row[6] = responseList[11]
		row[7] = responseList[9]
		row[8] = responseList[41]
		row[9] = responseList[42]
		row[10] = responseList[43]
		row[11] = responseList[44]
		row[12] = responseList[45]
		row[13] = responseList[46]
		row[14] = responseList[47]
		row[15] = responseList[48]
		row[16] = responseList[49]
		row[17] = responseList[50]
		row[18] = responseList[51]
		cursor.updateRow(row)
		# the following updates the progress bar
		arcpy.SetProgressorPosition()
	end_time = (time.time()-start_time)
        if(success == 1):
            arcpy.AddMessage(" ")
            arcpy.AddMessage("Processing complete: " + str(totalRows) + " records")
            arcpy.AddMessage("Processing speed: " + str("%.2f" % (totalRows/end_time)) + " records/per second")
            arcpy.AddMessage(" ")
            if(arcpy.GetParameter(6)):
                    arcpy.AddMessage("User elected to create display layer")
                    try:
                            x_coords = "Longitude"
                            y_coords = "Latitude"
                            z_coords = "POINT_Z"
                            out_Layer = "Data_Points"
                            saved_Layer = r"Data_Points"
                            
                            #if(arcpy.GetParameterAsText(7) == ""):
                            #	saved_Layer = r"display_points2"
                            #else:
                            #	saved_Layer = arcpy.GetParameterAsText(7)
                            
                            # Set the spatial reference
                            spRef = r"Coordinate Systems\Geographic Coordinate System\World\WGS 1984" 
                            #spRef = r"Coordinate Systems\Projected Coordinate Systems\World\WGS_1984_Web_Mercator_Auxiliary_Sphere.prj"
                            #spRef = arcpy.GetParameter(7)			
                            #spRef = r"Coordinate Systems\Projected Coordinate Systems\Utm\Nad 1983\NAD 1983 UTM Zone 11N.prj"					

                            # Make the XY event layer...
                            for i in range(1,10):
                                    try:
                                            arcpy.MakeXYEventLayer_management(tablePath, x_coords, y_coords, out_Layer, spRef)
                                            arcpy.SaveToLayerFile_management(out_Layer, saved_Layer)
                                            mxd = arcpy.mapping.MapDocument("CURRENT")
                                            dataFrame = arcpy.mapping.ListDataFrames(mxd, "*")[0]
                                            addlayer = arcpy.mapping.Layer(out_Layer)
                                            break
                                    except:
                                            if("ERROR 000725" in str(arcpy.GetMessage(3)) ):
                                                    saved_Layer_new = saved_Layer+"_"+str(i)
                                                    out_Layer_new = out_Layer+"_"+str(i)						
                                                    try:
                                                        arcpy.MakeXYEventLayer_management(tablePath, x_coords, y_coords, out_Layer_new, spRef)
                                                        arcpy.SaveToLayerFile_management(out_Layer_new, saved_Layer_new)
                                                        mxd = arcpy.mapping.MapDocument("CURRENT")
                                                        dataFrame = arcpy.mapping.ListDataFrames(mxd, "*")[0]
                                                        addlayer = arcpy.mapping.Layer(out_Layer_new)
                                                        break
                                                    except:
                                                        if(i==1):
                                                            arcpy.AddMessage("Data Layer "+ out_Layer + " already exists, trying: "+out_Layer_new)
                                                            arcpy.AddMessage(" ")
                                                        else:
                                                            arcpy.AddMessage("Data Layer "+ out_Layer_new + " already exists, trying: "+out_Layer+"_"+str(i+1))
                                                            arcpy.AddMessage(" ")

                            # Save to a layer file			
                            arcpy.mapping.AddLayer(dataFrame, addlayer, "TOP")

                    except:
                            #if error occured print message to screen
                            arcpy.AddMessage(arcpy.GetMessages())
            else:
                    arcpy.AddMessage("User elected not to create display layer")
        else:
		arcpy.AddWarning("Please ensure your api key is entered correctly")

#This resets the progress bar
arcpy.ResetProgressor()




