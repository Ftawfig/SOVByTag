var cc = DataStudioApp.createCommunityConnector();
var DEFAULT_PACKAGE = 'googleapis';

// https://developers.google.com/datastudio/connector/reference#getconfig
function getConfig() {
  var config = cc.getConfig();

  config
    .newInfo()
    .setId('instructions')
    .setText(
      'Enter your keyword.com API token to authenticate'
    );

  config
    .newTextInput()
    .setId('api_key')
    .setName('Keyword.com API Token')
    .setPlaceholder('xxxxxxxxxxxxxx')
    .setAllowOverride(false);

  config
    .newTextInput()
    .setId('project_id')
    .setName('Keyword.com Project ID #')
    .setPlaceholder('12345')
    .setAllowOverride(true);

  var option1 = config.newOptionBuilder()
  .setLabel("7 days")
  .setValue("7");

  var option2 = config.newOptionBuilder()
  .setLabel("30 days")
  .setValue("30");

  var option3 = config.newOptionBuilder()
  .setLabel("90 days")
  .setValue("90");

  var option4 = config.newOptionBuilder()
  .setLabel("180 days")
  .setValue("180");

  var option5 = config.newOptionBuilder()
  .setLabel("365 days")
  .setValue("365");

  config
    .newSelectSingle()
    .setId('daysRange')
    .setName('Date Range')
    .setHelpText('Select the date range for the requests')
    .setAllowOverride(true)
    .addOption(option1)
    .addOption(option2)
    .addOption(option3)
    .addOption(option4)
    .addOption(option5);


  //config.setDateRangeRequired(true);

  return config.build();
}

function getFields() {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  
  fields
    .newDimension()
    .setId('project_id')
    .setName('Project ID')
    .setType(types.NUMBER);

  fields
    .newDimension()
    .setId('project_name')
    .setName('Project Name')
    .setType(types.TEXT);

  fields
    .newDimension()
    .setId('project_keywords_count')
    .setName('Project Keywords Count')
    .setType(types.NUMBER);

  fields
    .newDimension()
    .setId('project_tags_count')
    .setName('Project Tags Count')
    .setType(types.NUMBER);

  fields
    .newDimension()
    .setId('tag_id')
    .setName('Tag ID')
    .setType(types.NUMBER);

  fields
    .newDimension()
    .setId('tag_name')
    .setName('Tag Name')
    .setType(types.TEXT);

  fields
    .newDimension()
    .setId('tag_keywords_count')
    .setName('Tag Keywords Count')
    .setType(types.NUMBER);

  fields
    .newDimension()
    .setId('tag_created_at')
    .setName('Tag Created At')
    .setType(types.TEXT);

  fields
    .newDimension()
    .setId('tag_updated_at')
    .setName('Tag Updated At')
    .setType(types.TEXT);

  fields
    .newDimension()
    .setId('date')
    .setName('Date')
    .setType(types.TEXT);

  fields
    .newDimension()
    .setId('url')
    .setName('URL')
    .setType(types.TEXT);

  fields
    .newMetric()
    .setId('clicks')
    .setName('Clicks')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  fields
    .newMetric()
    .setId('share')
    .setName('Share')
    .setType(types.PERCENT)
    .setAggregation(aggregations.SUM);

  return fields;
}

// https://developers.google.com/datastudio/connector/reference#getschema
function getSchema(request) {
  return {schema: getFields().build()};
}

function validateConfig(configParams) {
  configParams = configParams || {};
  configParams.package = configParams.package || DEFAULT_PACKAGE;

  configParams.package = configParams.package
    .split(',')
    .map(function(x) {
      return x.trim();
    })
    .join(',');

  return configParams;
}

function getDataForTag(projectData, tag) {
  var tagData = {
    "tag_id": tag.id,
    "tag_name": tag.name,
    "tag_created_at": tag.created_at,
    "tag_updated_at": tag.updated_at,
    "tag_keywords_count": tag.keywords_count
  };

  Object.keys(projectData).map(function(attribute) {
    tagData[attribute] = projectData[attribute];
  });

  return tagData;
}

/**
 * getDataForProject(): flattens the data for a project and its tags into a tabular format
 */
function getDataForProject(project) {  
  var rows = [];
  var projectData = {
    "project_id" : project.project_id,
    "project_name" : project.name,
    "project_keywords_count" : project.keywords_count["ACTIVE"],
    "project_tags_count" : project.tags_count
  };

  // if the project has tags, get the data for each tag. If not, then just assign all the tag fields as null.
  if (project.tags_count > 0) {
    project.tags.map(function(tag) {
      rows.push(getDataForTag(projectData, tag));
    })
  } else {
    projectData["tag_id"] = null;
    projectData["tag_name"] = null;
    projectData["tag_created_at"] = null;
    projectData["tag_updated_at"] = null;
    projectData["tag_keywords_count"] = null;

    rows.push(projectData);
  }

  //console.log(rows);

  return rows;
}


// https://developers.google.com/datastudio/connector/reference#getdata
function getData(request) {

  // fetch the group data
  try {
    var groupsResponse = fetchGroups(request);
    var normalizeGroupsResponse = normalizeResponse(groupsResponse);

    var groupsTable = [];

    // filter the response so that we're only retrieving data for the target project
    normalizeGroupsResponse = normalizeGroupsResponse.filter(function(project) {
      console.log(project);
      return project.attributes.project_id == request.configParams.project_id;
    });

    console.log(normalizeGroupsResponse);

    normalizeGroupsResponse.map(function(project) {

      var projectData = getDataForProject(project.attributes);

      groupsTable = groupsTable.concat(projectData);
    });

    console.log(groupsTable);

  } catch (e) {
    cc.newUserError()
      .setDebugText('Error fetching project and tag list data from API. Exception details: ' + e)
      .setText(
        'The connector has encountered an unrecoverable error. Please screenshot this error popup and contact Fadi Tawfig (faditawfig@gmail.com) if this error persists. API RESPONSE: ' + groupsResponse
      )
      .throwException();
  }

  request.configParams = validateConfig(request.configParams);

  var requestedFields = getFields().forIds(
    request.fields.map(function(field) {
      return field.name;
    })
  );

  var finalTable = [];

  // get the SOV data for each tag/project in the groups table
  try {
    groupsTable.map(function(group) {
      var apiResponse = fetchSOVData(request, group);
      var normalizedResponse = normalizeResponse(apiResponse);
      var sovData = getFormattedData(normalizedResponse, group, requestedFields);

      finalTable = finalTable.concat(sovData);

    })
  } catch (e) {
    cc.newUserError()
      .setDebugText('Error fetching SOV data from API. Exception details: ' + e)
      .setText(
        'The connector has encountered an unrecoverable error. Please screenshot this error popup and contact Fadi Tawfig (faditawfig@gmail.com) if this error persists. DATA RETRIEVED: ' + finalTable
      )
      .throwException();
  }

  return {
    schema: requestedFields.build(),
    rows: finalTable
  };
}

/**
 * fetchGroups(request): sends an API request to the keyword.com API to retrieve the list of active projects and tags within the account and returns the resposne.
 */
function fetchGroups(request) {
  var url = [
    'https://app.keyword.com/api/v2/groups/active?api_token=',
    request.configParams.api_key,
  ].join('');

  var response = UrlFetchApp.fetch(url);
  return response;
}

function fetchSOVData(request, group) {
  var url = '';
  
  if (group.tag_id) {
    url = [
      'https://app.keyword.com/api/v2/projects/',
      group.project_id,
      '/mindshare/from-cache?&daysrange=',
      request.configParams.daysRange,
      '&api_token=',
      request.configParams.api_key,
      '&tagId=',
      group.tag_id
    ].join('');
  } else   {
     url = [
      'https://app.keyword.com/api/v2/projects/',
      group.project_id,
      '/mindshare/from-cache?&daysrange=',
      request.configParams.daysRange,
      '&api_token=',
      request.configParams.api_key
    ].join('');
  }

  var response = UrlFetchApp.fetch(url);
  return response;
}


/**
 * Parses response JSON string and returns it as an object.
 */
function normalizeResponse(responseString) {
  var response = JSON.parse(responseString);
  var dataObject = response.data;

  return dataObject;
}

/**
 * Formats a single row of data into the required format.
 *
 * @param {Object} requestedFields Fields requested in the getData request.
 * @param {string} packageName Name of the package who's download data is being
 *    processed.
 * @param {Object} dailyDownload Contains the download data for a certain day.
 * @returns {Object} Contains values for requested fields in predefined format.
 */
function formatData(requestedFields, dailyData, date, group) {
  var row = requestedFields.asArray().map(function(requestedField) {
    switch (requestedField.getId()) {
      case 'project_id':
        return group.project_id;
      case 'project_name':
        return group.project_name;
      case 'project_keywords_count':
        return group.project_keywords_count;
      case 'project_tags_count':
        return group.project_tags_count;
      case 'tag_id':
        return group.tag_id;
      case 'tag_name':
        return group.tag_name;
      case 'tag_keywords_count':
        return group.tag_keywords_count;
      case 'tag_created_at':
        return group.tag_created_at;
      case 'tag_updated_at':
        return group.tag_updated_at;
      case 'date':
        return date;
      case 'url':
        return dailyData.url;
      case 'tagId':
        return 23015;
      case 'clicks':
        return dailyData.clicks;
      case 'share':
        return dailyData.percentage / 100;
      default:
        return '';
    }
  });
  //console.log(row);

  return {values: row};
}


/**
 * Formats the parsed response from external data source into correct tabular
 * format and returns only the requestedFields
 *
 * @param {Object} parsedResponse The response string from external data source
 *     parsed into an object in a standard format.
 * @param {Array} requestedFields The fields requested in the getData request.
 * @returns {Array} Array containing rows of data in key-value pairs for each
 *     field.
 */
function getFormattedData(response, groupData, requestedFields) {
  var data = [];

  Object.keys(response).map(function(date) {
    var dateData = response[date];

    //console.log(dateData);

    var formattedData = dateData.map(function(dailyData) {
      return formatData(requestedFields, dailyData, date, groupData);
    });

    data = data.concat(formattedData);
  });

  return data;
}

function isAdminUser() {
  return true;
}