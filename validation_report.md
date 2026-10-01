# CRL Master DB - end-to-end reconciliation

Generated: 2026-10-01 17:11:09 IST
Source counted as of: 2026-10-01 17:01:45 IST (start of the latest pipeline run; later source records belong to the next run)
Target Live Records = latest version of each record, not flagged DELETED (view `<table>__current`).

| Source Table | Target Table | Source Row Count | Target Live Records | Difference | Status | Deleted (flagged) | Superseded versions | Total rows (all versions) | Note |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- |
| `mongodb_elevate_atlas: elevate-diksha.deletionAuditLogs` | `application.elevate_atlas__elevate_diksha__deletionauditlogs` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: elevate-diksha.entities` | `application.elevate_atlas__elevate_diksha__entities` | 107 | 107 | 0 | PASS | 0 | 0 | 107 | exact match |
| `mongodb_elevate_atlas: elevate-diksha.entityTypes` | `application.elevate_atlas__elevate_diksha__entitytypes` | 10 | 10 | 0 | PASS | 0 | 0 | 10 | exact match |
| `mongodb_elevate_atlas: elevate-diksha.userRoleExtension` | `application.elevate_atlas__elevate_diksha__userroleextension` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: elevate-notification.deletionAuditLogs` | `application.elevate_atlas__elevate_notification__deletionauditlogs` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: elevate-notification.deviceTokens` | `application.elevate_atlas__elevate_notification__devicetokens` | 1 | 1 | 0 | PASS | 0 | 0 | 1 | exact match |
| `mongodb_elevate_atlas: elevate-notification.entities` | `application.elevate_atlas__elevate_notification__entities` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: elevate-notification.entityTypes` | `application.elevate_atlas__elevate_notification__entitytypes` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: elevate-notification.notifications` | `application.elevate_atlas__elevate_notification__notifications` | 13 | 13 | 0 | PASS | 0 | 1 | 14 | exact match |
| `mongodb_elevate_atlas: elevate-notification.userRoleExtension` | `application.elevate_atlas__elevate_notification__userroleextension` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: Mobident.bookings` | `application.elevate_atlas__mobident__bookings` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: Mobident.otps` | `application.elevate_atlas__mobident__otps` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: Mobident.profiles` | `application.elevate_atlas__mobident__profiles` | 63 | 63 | 0 | PASS | 0 | 0 | 63 | exact match |
| `mongodb_elevate_atlas: Mobident.scanprogresses` | `application.elevate_atlas__mobident__scanprogresses` | 20 | 20 | 0 | PASS | 0 | 0 | 20 | exact match |
| `mongodb_elevate_atlas: Mobident.screenings` | `application.elevate_atlas__mobident__screenings` | 66 | 66 | 0 | PASS | 0 | 0 | 66 | exact match |
| `mongodb_elevate_atlas: Mobident.sessions` | `application.elevate_atlas__mobident__sessions` | 3 | 3 | 0 | PASS | 0 | 0 | 3 | exact match |
| `mongodb_elevate_atlas: Mobident.users` | `application.elevate_atlas__mobident__users` | 44 | 44 | 0 | PASS | 0 | 0 | 44 | exact match |
| `mongodb_elevate_atlas: project.certificateTemplates` | `application.elevate_atlas__project__certificatetemplates` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: project.configurations` | `application.elevate_atlas__project__configurations` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: project.deletionAuditLogs` | `application.elevate_atlas__project__deletionauditlogs` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: project.forms` | `application.elevate_atlas__project__forms` | 3 | 3 | 0 | PASS | 0 | 0 | 3 | exact match |
| `mongodb_elevate_atlas: project.organizationExtension` | `application.elevate_atlas__project__organizationextension` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: project.programs` | `application.elevate_atlas__project__programs` | 69 | 69 | 0 | PASS | 1 | 0 | 70 | exact match |
| `mongodb_elevate_atlas: project.programUsers` | `application.elevate_atlas__project__programusers` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: project.projectAttributes` | `application.elevate_atlas__project__projectattributes` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: project.projectCategories` | `application.elevate_atlas__project__projectcategories` | 7 | 7 | 0 | PASS | 0 | 0 | 7 | exact match |
| `mongodb_elevate_atlas: project.projects` | `application.elevate_atlas__project__projects` | 1 | 1 | 0 | PASS | 1 | 0 | 2 | exact match |
| `mongodb_elevate_atlas: project.projectTemplates` | `application.elevate_atlas__project__projecttemplates` | 24 | 24 | 0 | PASS | 0 | 0 | 24 | exact match |
| `mongodb_elevate_atlas: project.projectTemplateTasks` | `application.elevate_atlas__project__projecttemplatetasks` | 82 | 82 | 0 | PASS | 0 | 0 | 82 | exact match |
| `mongodb_elevate_atlas: project.rollouts` | `application.elevate_atlas__project__rollouts` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: project.solutions` | `application.elevate_atlas__project__solutions` | 1 | 1 | 0 | PASS | 1 | 0 | 2 | exact match |
| `mongodb_elevate_atlas: project.userCourses` | `application.elevate_atlas__project__usercourses` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: project.userExtension` | `application.elevate_atlas__project__userextension` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.businessPlanAuditEvents` | `application.elevate_atlas__samiksha__businessplanauditevents` | 168 | 168 | 0 | PASS | 0 | 0 | 168 | exact match |
| `mongodb_elevate_atlas: samiksha.businessPlanMilestones` | `application.elevate_atlas__samiksha__businessplanmilestones` | 23 | 23 | 0 | PASS | 0 | 0 | 23 | exact match |
| `mongodb_elevate_atlas: samiksha.businessPlanValidations` | `application.elevate_atlas__samiksha__businessplanvalidations` | 9 | 9 | 0 | PASS | 0 | 0 | 9 | exact match |
| `mongodb_elevate_atlas: samiksha.businessPlanVersions` | `application.elevate_atlas__samiksha__businessplanversions` | 9 | 9 | 0 | PASS | 0 | 0 | 9 | exact match |
| `mongodb_elevate_atlas: samiksha.coachProfiles` | `application.elevate_atlas__samiksha__coachprofiles` | 14 | 14 | 0 | PASS | 0 | 0 | 14 | exact match |
| `mongodb_elevate_atlas: samiksha.configurations` | `application.elevate_atlas__samiksha__configurations` | 1 | 1 | 0 | PASS | 0 | 0 | 1 | exact match |
| `mongodb_elevate_atlas: samiksha.criteria` | `application.elevate_atlas__samiksha__criteria` | 10 | 10 | 0 | PASS | 0 | 0 | 10 | exact match |
| `mongodb_elevate_atlas: samiksha.criteriaQuestions` | `application.elevate_atlas__samiksha__criteriaquestions` | 10 | 10 | 0 | PASS | 0 | 0 | 10 | exact match |
| `mongodb_elevate_atlas: samiksha.deletionAuditLogs` | `application.elevate_atlas__samiksha__deletionauditlogs` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.entities` | `application.elevate_atlas__samiksha__entities` | 2 | 2 | 0 | PASS | 0 | 0 | 2 | exact match |
| `mongodb_elevate_atlas: samiksha.entityAssessors` | `application.elevate_atlas__samiksha__entityassessors` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.entityAssessorsTrackers` | `application.elevate_atlas__samiksha__entityassessorstrackers` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.entityTypes` | `application.elevate_atlas__samiksha__entitytypes` | 2 | 2 | 0 | PASS | 0 | 0 | 2 | exact match |
| `mongodb_elevate_atlas: samiksha.entrepreneurProfiles` | `application.elevate_atlas__samiksha__entrepreneurprofiles` | 21 | 21 | 0 | PASS | 0 | 0 | 21 | exact match |
| `mongodb_elevate_atlas: samiksha.feedback` | `application.elevate_atlas__samiksha__feedback` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.forms` | `application.elevate_atlas__samiksha__forms` | 2 | 2 | 0 | PASS | 0 | 0 | 2 | exact match |
| `mongodb_elevate_atlas: samiksha.frameworks` | `application.elevate_atlas__samiksha__frameworks` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.growthStageProgress` | `application.elevate_atlas__samiksha__growthstageprogress` | 2 | 2 | 0 | PASS | 0 | 0 | 2 | exact match |
| `mongodb_elevate_atlas: samiksha.insights` | `application.elevate_atlas__samiksha__insights` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.journeyCatalog` | `application.elevate_atlas__samiksha__journeycatalog` | 5 | 5 | 0 | PASS | 0 | 0 | 5 | exact match |
| `mongodb_elevate_atlas: samiksha.journeySuggestions` | `application.elevate_atlas__samiksha__journeysuggestions` | 73 | 73 | 0 | PASS | 0 | 0 | 73 | exact match |
| `mongodb_elevate_atlas: samiksha.libraryCategories` | `application.elevate_atlas__samiksha__librarycategories` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.mediaFiles` | `application.elevate_atlas__samiksha__mediafiles` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.mediaUploads` | `application.elevate_atlas__samiksha__mediauploads` | 319 | 319 | 0 | PASS | 0 | 0 | 319 | exact match |
| `mongodb_elevate_atlas: samiksha.migrations` | `application.elevate_atlas__samiksha__migrations` | 7 | 7 | 0 | PASS | 0 | 0 | 7 | exact match |
| `mongodb_elevate_atlas: samiksha.observationAnalytics` | `application.elevate_atlas__samiksha__observationanalytics` | 10 | 10 | 0 | PASS | 0 | 0 | 10 | exact match |
| `mongodb_elevate_atlas: samiksha.observations` | `application.elevate_atlas__samiksha__observations` | 90 | 90 | 0 | PASS | 0 | 0 | 90 | exact match |
| `mongodb_elevate_atlas: samiksha.observationSubmissions` | `application.elevate_atlas__samiksha__observationsubmissions` | 233 | 233 | 0 | PASS | 0 | 0 | 233 | exact match |
| `mongodb_elevate_atlas: samiksha.organizationExtension` | `application.elevate_atlas__samiksha__organizationextension` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.polls` | `application.elevate_atlas__samiksha__polls` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.pollSubmissions` | `application.elevate_atlas__samiksha__pollsubmissions` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.profileQuestionResponses` | `application.elevate_atlas__samiksha__profilequestionresponses` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.profileQuestionSets` | `application.elevate_atlas__samiksha__profilequestionsets` | 4 | 4 | 0 | PASS | 0 | 0 | 4 | exact match |
| `mongodb_elevate_atlas: samiksha.programs` | `application.elevate_atlas__samiksha__programs` | 3 | 3 | 0 | PASS | 0 | 0 | 3 | exact match |
| `mongodb_elevate_atlas: samiksha.programUsers` | `application.elevate_atlas__samiksha__programusers` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.questions` | `application.elevate_atlas__samiksha__questions` | 512 | 512 | 0 | PASS | 0 | 0 | 512 | exact match |
| `mongodb_elevate_atlas: samiksha.recommendationTargets` | `application.elevate_atlas__samiksha__recommendationtargets` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.reportOptions` | `application.elevate_atlas__samiksha__reportoptions` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.sharedLink` | `application.elevate_atlas__samiksha__sharedlink` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.solutions` | `application.elevate_atlas__samiksha__solutions` | 80 | 80 | 0 | PASS | 0 | 0 | 80 | exact match |
| `mongodb_elevate_atlas: samiksha.staticLinks` | `application.elevate_atlas__samiksha__staticlinks` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.submissions` | `application.elevate_atlas__samiksha__submissions` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.surveys` | `application.elevate_atlas__samiksha__surveys` | 18 | 18 | 0 | PASS | 0 | 0 | 18 | exact match |
| `mongodb_elevate_atlas: samiksha.surveySubmissions` | `application.elevate_atlas__samiksha__surveysubmissions` | 1 | 1 | 0 | PASS | 0 | 0 | 1 | exact match |
| `mongodb_elevate_atlas: samiksha.taskEvidence` | `application.elevate_atlas__samiksha__taskevidence` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.userCourses` | `application.elevate_atlas__samiksha__usercourses` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.userExtension` | `application.elevate_atlas__samiksha__userextension` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: samiksha.userRoles` | `application.elevate_atlas__samiksha__userroles` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: scp_service.resources` | `application.elevate_atlas__scp_service__resources` | 1 | 1 | 0 | PASS | 0 | 0 | 1 | exact match |
| `mongodb_network_data: crl_catalog_adaptor.catalog_items` | `application.network_data__crl_catalog_adaptor__catalog_items` | 1115 | 1115 | 0 | PASS | 0 | 0 | 1115 | exact match |
| `mongodb_network_data: crl_catalog_adaptor.interests` | `application.network_data__crl_catalog_adaptor__interests` | 3 | 3 | 0 | PASS | 0 | 0 | 3 | exact match |
| `mongodb_network_data: crl_catalog_adaptor.outbox` | `application.network_data__crl_catalog_adaptor__outbox` | 2758 | 2758 | 0 | PASS | 0 | 0 | 2758 | exact match |
| `mongodb_network_data: crl_catalog_adaptor.publications` | `application.network_data__crl_catalog_adaptor__publications` | 2508 | 2508 | 0 | PASS | 0 | 0 | 2508 | exact match |
| `mongodb_network_data: crl_catalog_adaptor.selco_items` | `application.network_data__crl_catalog_adaptor__selco_items` | 704 | 704 | 0 | PASS | 0 | 0 | 704 | exact match |
| `mongodb_network_data: crl_catalog_adaptor.selco_sync_runs` | `application.network_data__crl_catalog_adaptor__selco_sync_runs` | 6 | 6 | 0 | PASS | 0 | 0 | 6 | exact match |
| `mongodb_network_data: crl_discovery_adaptor.assignments` | `application.network_data__crl_discovery_adaptor__assignments` | 4 | 4 | 0 | PASS | 0 | 0 | 4 | exact match |
| `mongodb_network_data: crl_discovery_adaptor.discovery_index` | `application.network_data__crl_discovery_adaptor__discovery_index` | 1113 | 1113 | 0 | PASS | 0 | 0 | 1113 | exact match |
| `mongodb_network_data: crl_discovery_adaptor.discovery_requests` | `application.network_data__crl_discovery_adaptor__discovery_requests` | 1051 | 1051 | 0 | PASS | 0 | 0 | 1051 | exact match |
| `mongodb_network_data: crl_discovery_adaptor.discovery_sync_state` | `application.network_data__crl_discovery_adaptor__discovery_sync_state` | 6 | 6 | 0 | PASS | 0 | 1 | 7 | exact match |
| `mongodb_network_data: demandmarketplacebap.callbackresults` | `application.network_data__demandmarketplacebap__callbackresults` | 561 | 561 | 0 | PASS | 0 | 0 | 561 | exact match |
| `mongodb_network_data: demandmarketplacebap.failedtransactions` | `application.network_data__demandmarketplacebap__failedtransactions` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_network_data: demandmarketplacebap.productcatalog` | `application.network_data__demandmarketplacebap__productcatalog` | 149 | 149 | 0 | PASS | 0 | 0 | 149 | exact match |
| `mongodb_network_data: demandmarketplacebap.transactions` | `application.network_data__demandmarketplacebap__transactions` | 1322 | 1322 | 0 | PASS | 0 | 0 | 1322 | exact match |
| `mongodb_network_data: demandmarketplacebpp.demand_interests` | `application.network_data__demandmarketplacebpp__demand_interests` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_network_data: demandmarketplacebpp.demand_products` | `application.network_data__demandmarketplacebpp__demand_products` | 718 | 718 | 0 | PASS | 0 | 0 | 718 | exact match |
| `mongodb_network_data: demandmarketplacebpp.orders` | `application.network_data__demandmarketplacebpp__orders` | 32 | 32 | 0 | PASS | 0 | 0 | 32 | exact match |
| `mongodb_network_data: demandmarketplacebpp.publish_transactions` | `application.network_data__demandmarketplacebpp__publish_transactions` | 720 | 720 | 0 | PASS | 0 | 0 | 720 | exact match |
| `mongodb_network_data: demandmarketplacebpp.schema_versions` | `application.network_data__demandmarketplacebpp__schema_versions` | 4 | 4 | 0 | PASS | 0 | 0 | 4 | exact match |
| `mongodb_network_data: financebapbusiness.callbackresults` | `application.network_data__financebapbusiness__callbackresults` | 507 | 507 | 0 | PASS | 0 | 0 | 507 | exact match |
| `mongodb_network_data: financebapbusiness.failedtransactions` | `application.network_data__financebapbusiness__failedtransactions` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_network_data: financebapbusiness.schemeassignments` | `application.network_data__financebapbusiness__schemeassignments` | 21 | 21 | 0 | PASS | 0 | 0 | 21 | exact match |
| `mongodb_network_data: financebapbusiness.schemecatalog` | `application.network_data__financebapbusiness__schemecatalog` | 52 | 52 | 0 | PASS | 0 | 0 | 52 | exact match |
| `mongodb_network_data: financebapbusiness.transactions` | `application.network_data__financebapbusiness__transactions` | 1092 | 1092 | 0 | PASS | 0 | 0 | 1092 | exact match |
| `mongodb_network_data: financebppbusiness.loan_applications` | `application.network_data__financebppbusiness__loan_applications` | 4 | 4 | 0 | PASS | 0 | 0 | 4 | exact match |
| `mongodb_network_data: financebppbusiness.loan_interests` | `application.network_data__financebppbusiness__loan_interests` | 2 | 2 | 0 | PASS | 0 | 0 | 2 | exact match |
| `mongodb_network_data: financebppbusiness.loans` | `application.network_data__financebppbusiness__loans` | 50 | 50 | 0 | PASS | 0 | 0 | 50 | exact match |
| `mongodb_network_data: financebppbusiness.publish_transactions` | `application.network_data__financebppbusiness__publish_transactions` | 135 | 135 | 0 | PASS | 0 | 0 | 135 | exact match |
| `mongodb_network_data: govtschemebapbusiness.callbackresults` | `application.network_data__govtschemebapbusiness__callbackresults` | 513 | 513 | 0 | PASS | 0 | 0 | 513 | exact match |
| `mongodb_network_data: govtschemebapbusiness.failedtransactions` | `application.network_data__govtschemebapbusiness__failedtransactions` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_network_data: govtschemebapbusiness.schemeassignments` | `application.network_data__govtschemebapbusiness__schemeassignments` | 8 | 8 | 0 | PASS | 0 | 0 | 8 | exact match |
| `mongodb_network_data: govtschemebapbusiness.schemecatalog` | `application.network_data__govtschemebapbusiness__schemecatalog` | 93 | 93 | 0 | PASS | 0 | 0 | 93 | exact match |
| `mongodb_network_data: govtschemebapbusiness.transactions` | `application.network_data__govtschemebapbusiness__transactions` | 1111 | 1111 | 0 | PASS | 0 | 0 | 1111 | exact match |
| `mongodb_network_data: govtschemebppbusiness.govt_scheme_applications` | `application.network_data__govtschemebppbusiness__govt_scheme_applications` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_network_data: govtschemebppbusiness.govt_scheme_interests` | `application.network_data__govtschemebppbusiness__govt_scheme_interests` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_network_data: govtschemebppbusiness.govt_schemes` | `application.network_data__govtschemebppbusiness__govt_schemes` | 92 | 92 | 0 | PASS | 0 | 0 | 92 | exact match |
| `mongodb_network_data: govtschemebppbusiness.publish_transactions` | `application.network_data__govtschemebppbusiness__publish_transactions` | 94 | 94 | 0 | PASS | 0 | 0 | 94 | exact match |
| `mongodb_network_data: learningbapbusiness.callbackresults` | `application.network_data__learningbapbusiness__callbackresults` | 501 | 501 | 0 | PASS | 0 | 0 | 501 | exact match |
| `mongodb_network_data: learningbapbusiness.courseassignments` | `application.network_data__learningbapbusiness__courseassignments` | 7 | 7 | 0 | PASS | 0 | 0 | 7 | exact match |
| `mongodb_network_data: learningbapbusiness.coursecatalog` | `application.network_data__learningbapbusiness__coursecatalog` | 162 | 162 | 0 | PASS | 0 | 0 | 162 | exact match |
| `mongodb_network_data: learningbapbusiness.failedtransactions` | `application.network_data__learningbapbusiness__failedtransactions` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_network_data: learningbapbusiness.transactions` | `application.network_data__learningbapbusiness__transactions` | 949 | 949 | 0 | PASS | 0 | 0 | 949 | exact match |
| `mongodb_network_data: learningbppbusiness.course_interests` | `application.network_data__learningbppbusiness__course_interests` | 4 | 4 | 0 | PASS | 0 | 0 | 4 | exact match |
| `mongodb_network_data: learningbppbusiness.courses` | `application.network_data__learningbppbusiness__courses` | 160 | 160 | 0 | PASS | 0 | 0 | 160 | exact match |
| `mongodb_network_data: learningbppbusiness.providers` | `application.network_data__learningbppbusiness__providers` | 134 | 134 | 0 | PASS | 0 | 0 | 134 | exact match |
| `mongodb_network_data: learningbppbusiness.publish_transactions` | `application.network_data__learningbppbusiness__publish_transactions` | 398 | 398 | 0 | PASS | 0 | 0 | 398 | exact match |
| `mongodb_network_data: marketplace-bap.transactions` | `application.network_data__marketplace_bap__transactions` | 166 | 166 | 0 | PASS | 0 | 0 | 166 | exact match |
| `mongodb_network_data: marketplace-bap.users` | `application.network_data__marketplace_bap__users` | 11 | 11 | 0 | PASS | 0 | 0 | 11 | exact match |
| `mongodb_network_data: marketplace-bpp.cooperatives` | `application.network_data__marketplace_bpp__cooperatives` | 6 | 6 | 0 | PASS | 0 | 0 | 6 | exact match |
| `mongodb_network_data: marketplace-bpp.counters` | `application.network_data__marketplace_bpp__counters` | 1 | 1 | 0 | PASS | 0 | 0 | 1 | exact match |
| `mongodb_network_data: marketplace-bpp.interests` | `application.network_data__marketplace_bpp__interests` | 1 | 1 | 0 | PASS | 0 | 0 | 1 | exact match |
| `mongodb_network_data: marketplace-bpp.orders` | `application.network_data__marketplace_bpp__orders` | 81 | 81 | 0 | PASS | 0 | 0 | 81 | exact match |
| `mongodb_network_data: marketplace-bpp.products` | `application.network_data__marketplace_bpp__products` | 45 | 45 | 0 | PASS | 0 | 0 | 45 | exact match |
| `mongodb_network_data: supplymarketplace.publish_transactions` | `application.network_data__supplymarketplace__publish_transactions` | 3 | 3 | 0 | PASS | 0 | 0 | 3 | exact match |
| `mongodb_network_data: supplymarketplace.schema_versions` | `application.network_data__supplymarketplace__schema_versions` | 1 | 1 | 0 | PASS | 0 | 0 | 1 | exact match |
| `mongodb_network_data: supplymarketplace.supply_interests` | `application.network_data__supplymarketplace__supply_interests` | 1 | 1 | 0 | PASS | 0 | 0 | 1 | exact match |
| `mongodb_network_data: supplymarketplace.supply_products` | `application.network_data__supplymarketplace__supply_products` | 3 | 3 | 0 | PASS | 0 | 0 | 3 | exact match |
| `mongodb_network_data: supplymarketplacebap.callbackresults` | `application.network_data__supplymarketplacebap__callbackresults` | 663 | 663 | 0 | PASS | 0 | 0 | 663 | exact match |
| `mongodb_network_data: supplymarketplacebap.failedtransactions` | `application.network_data__supplymarketplacebap__failedtransactions` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_network_data: supplymarketplacebap.interests` | `application.network_data__supplymarketplacebap__interests` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_network_data: supplymarketplacebap.productcatalog` | `application.network_data__supplymarketplacebap__productcatalog` | 84 | 84 | 0 | PASS | 0 | 0 | 84 | exact match |
| `mongodb_network_data: supplymarketplacebap.transactions` | `application.network_data__supplymarketplacebap__transactions` | 1974 | 1974 | 0 | PASS | 0 | 0 | 1974 | exact match |
| `mongodb_network_data: supplymarketplacebpp.publish_transactions` | `application.network_data__supplymarketplacebpp__publish_transactions` | 95 | 95 | 0 | PASS | 0 | 0 | 95 | exact match |
| `mongodb_network_data: supplymarketplacebpp.schema_versions` | `application.network_data__supplymarketplacebpp__schema_versions` | 4 | 4 | 0 | PASS | 0 | 0 | 4 | exact match |
| `mongodb_network_data: supplymarketplacebpp.supply_interests` | `application.network_data__supplymarketplacebpp__supply_interests` | 2 | 2 | 0 | PASS | 0 | 0 | 2 | exact match |
| `mongodb_network_data: supplymarketplacebpp.supply_products` | `application.network_data__supplymarketplacebpp__supply_products` | 81 | 81 | 0 | PASS | 0 | 0 | 81 | exact match |
| `postgresql_supabase: mentoring.public.availabilities` | `application.supabase__mentoring__availabilities` | 3 | 3 | 0 | PASS | 0 | 0 | 3 | exact match |
| `postgresql_supabase: mentoring.public.connection_requests` | `application.supabase__mentoring__connection_requests` | 2 | 2 | 0 | PASS | 0 | 0 | 2 | exact match |
| `postgresql_supabase: mentoring.public.connections` | `application.supabase__mentoring__connections` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: mentoring.public.default_rules` | `application.supabase__mentoring__default_rules` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: mentoring.public.entities` | `application.supabase__mentoring__entities` | 30 | 30 | 0 | PASS | 0 | 0 | 30 | exact match |
| `postgresql_supabase: mentoring.public.entity_types` | `application.supabase__mentoring__entity_types` | 9 | 9 | 0 | PASS | 0 | 0 | 9 | exact match |
| `postgresql_supabase: mentoring.public.feedbacks` | `application.supabase__mentoring__feedbacks` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: mentoring.public.file_uploads` | `application.supabase__mentoring__file_uploads` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: mentoring.public.forms` | `application.supabase__mentoring__forms` | 12 | 12 | 0 | PASS | 0 | 0 | 12 | exact match |
| `postgresql_supabase: mentoring.public.issues` | `application.supabase__mentoring__issues` | 9 | 9 | 0 | PASS | 0 | 0 | 9 | exact match |
| `postgresql_supabase: mentoring.public.modules` | `application.supabase__mentoring__modules` | 27 | 27 | 0 | PASS | 0 | 0 | 27 | exact match |
| `postgresql_supabase: mentoring.public.notification_templates` | `application.supabase__mentoring__notification_templates` | 38 | 38 | 0 | PASS | 0 | 0 | 38 | exact match |
| `postgresql_supabase: mentoring.public.organization_extension` | `application.supabase__mentoring__organization_extension` | 1 | 1 | 0 | PASS | 0 | 0 | 1 | exact match |
| `postgresql_supabase: mentoring.public.permissions` | `application.supabase__mentoring__permissions` | 71 | 71 | 0 | PASS | 0 | 0 | 71 | exact match |
| `postgresql_supabase: mentoring.public.post_session_details` | `application.supabase__mentoring__post_session_details` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: mentoring.public.question_sets` | `application.supabase__mentoring__question_sets` | 2 | 2 | 0 | PASS | 0 | 0 | 2 | exact match |
| `postgresql_supabase: mentoring.public.questions` | `application.supabase__mentoring__questions` | 16 | 16 | 0 | PASS | 0 | 0 | 16 | exact match |
| `postgresql_supabase: mentoring.public.report_queries` | `application.supabase__mentoring__report_queries` | 14 | 14 | 0 | PASS | 0 | 0 | 14 | exact match |
| `postgresql_supabase: mentoring.public.report_role_mapping` | `application.supabase__mentoring__report_role_mapping` | 13 | 13 | 0 | PASS | 0 | 0 | 13 | exact match |
| `postgresql_supabase: mentoring.public.report_types` | `application.supabase__mentoring__report_types` | 5 | 5 | 0 | PASS | 0 | 0 | 5 | exact match |
| `postgresql_supabase: mentoring.public.reports` | `application.supabase__mentoring__reports` | 12 | 12 | 0 | PASS | 0 | 0 | 12 | exact match |
| `postgresql_supabase: mentoring.public.resources` | `application.supabase__mentoring__resources` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: mentoring.public.role_extensions` | `application.supabase__mentoring__role_extensions` | 6 | 6 | 0 | PASS | 0 | 0 | 6 | exact match |
| `postgresql_supabase: mentoring.public.role_permission_mapping` | `application.supabase__mentoring__role_permission_mapping` | 207 | 207 | 0 | PASS | 0 | 0 | 207 | exact match |
| `postgresql_supabase: mentoring.public.sequelize_meta` | `application.supabase__mentoring__sequelize_meta` | 160 | 160 | 0 | PASS | 0 | 0 | 160 | exact match |
| `postgresql_supabase: mentoring.public.session_attendees` | `application.supabase__mentoring__session_attendees` | 2 | 2 | 0 | PASS | 0 | 0 | 2 | exact match |
| `postgresql_supabase: mentoring.public.session_enrollments` | `application.supabase__mentoring__session_enrollments` | 5 | 5 | 0 | PASS | 0 | 0 | 5 | exact match |
| `postgresql_supabase: mentoring.public.session_ownerships` | `application.supabase__mentoring__session_ownerships` | 6 | 6 | 0 | PASS | 0 | 0 | 6 | exact match |
| `postgresql_supabase: mentoring.public.session_request` | `application.supabase__mentoring__session_request` | 2 | 2 | 0 | PASS | 0 | 0 | 2 | exact match |
| `postgresql_supabase: mentoring.public.session_request_mapping` | `application.supabase__mentoring__session_request_mapping` | 2 | 2 | 0 | PASS | 0 | 0 | 2 | exact match |
| `postgresql_supabase: mentoring.public.sessions` | `application.supabase__mentoring__sessions` | 3 | 3 | 0 | PASS | 0 | 0 | 3 | exact match |
| `postgresql_supabase: mentoring.public.user_extensions` | `application.supabase__mentoring__user_extensions` | 107 | 107 | 0 | PASS | 0 | 2 | 109 | exact match |
| `postgresql_supabase: postgres.public.entities` | `application.supabase__postgres__entities` | 268 | 268 | 0 | PASS | 0 | 0 | 268 | exact match |
| `postgresql_supabase: postgres.public.entity_types` | `application.supabase__postgres__entity_types` | 14 | 14 | 0 | PASS | 0 | 0 | 14 | exact match |
| `postgresql_supabase: postgres.public.feature_role_mapping` | `application.supabase__postgres__feature_role_mapping` | 7 | 7 | 0 | PASS | 0 | 0 | 7 | exact match |
| `postgresql_supabase: postgres.public.features` | `application.supabase__postgres__features` | 12 | 12 | 0 | PASS | 0 | 0 | 12 | exact match |
| `postgresql_supabase: postgres.public.file_uploads` | `application.supabase__postgres__file_uploads` | 9 | 9 | 0 | PASS | 0 | 0 | 9 | exact match |
| `postgresql_supabase: postgres.public.forms` | `application.supabase__postgres__forms` | 6 | 6 | 0 | PASS | 0 | 0 | 6 | exact match |
| `postgresql_supabase: postgres.public.invitations` | `application.supabase__postgres__invitations` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: postgres.public.modules` | `application.supabase__postgres__modules` | 37 | 37 | 0 | PASS | 0 | 0 | 37 | exact match |
| `postgresql_supabase: postgres.public.notification_templates` | `application.supabase__postgres__notification_templates` | 28 | 28 | 0 | PASS | 0 | 0 | 28 | exact match |
| `postgresql_supabase: postgres.public.permissions` | `application.supabase__postgres__permissions` | 50 | 50 | 0 | PASS | 0 | 0 | 50 | exact match |
| `postgresql_supabase: postgres.public.role_permission_mapping` | `application.supabase__postgres__role_permission_mapping` | 98 | 98 | 0 | PASS | 0 | 0 | 98 | exact match |
| `postgresql_supabase: postgres.public.sequelize_meta` | `application.supabase__postgres__sequelize_meta` | 140 | 140 | 0 | PASS | 0 | 0 | 140 | exact match |
| `postgresql_supabase: postgres.public.user_roles` | `application.supabase__postgres__user_roles` | 21 | 21 | 0 | PASS | 0 | 0 | 21 | exact match |
| `postgresql_supabase: postgres.public.user_sessions` | `application.supabase__postgres__user_sessions` | 1441 | 1441 | 0 | FAIL | 2 | 1 | 1444 | UNEXPLAINED: 1 source record(s) not live in target (absent or different content); UNEXPLAINED: 1 live target record(s) not in source (deletion not flagged or stale version) |
| `postgresql_supabase: postgres.public.users` | `application.supabase__postgres__users` | 136 | 136 | 0 | PASS | 1 | 0 | 137 | exact match |
| `postgresql_supabase: postgres.public.users_credentials` | `application.supabase__postgres__users_credentials` | 65 | 65 | 0 | PASS | 0 | 0 | 65 | exact match |
| `postgresql_supabase: scp.public.actions` | `application.supabase__scp__actions` | 26 | 26 | 0 | PASS | 0 | 0 | 26 | exact match |
| `postgresql_supabase: scp.public.activities` | `application.supabase__scp__activities` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: scp.public.certificate_base_templates` | `application.supabase__scp__certificate_base_templates` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: scp.public.comments` | `application.supabase__scp__comments` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: scp.public.entities` | `application.supabase__scp__entities` | 31 | 31 | 0 | PASS | 0 | 0 | 31 | exact match |
| `postgresql_supabase: scp.public.entities_model_mapping` | `application.supabase__scp__entities_model_mapping` | 50 | 50 | 0 | PASS | 0 | 0 | 50 | exact match |
| `postgresql_supabase: scp.public.entity_types` | `application.supabase__scp__entity_types` | 36 | 36 | 0 | PASS | 0 | 0 | 36 | exact match |
| `postgresql_supabase: scp.public.forms` | `application.supabase__scp__forms` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: scp.public.modules` | `application.supabase__scp__modules` | 26 | 26 | 0 | PASS | 0 | 0 | 26 | exact match |
| `postgresql_supabase: scp.public.organization_configs` | `application.supabase__scp__organization_configs` | 1 | 1 | 0 | PASS | 0 | 0 | 1 | exact match |
| `postgresql_supabase: scp.public.organization_extensions` | `application.supabase__scp__organization_extensions` | 5 | 5 | 0 | PASS | 0 | 0 | 5 | exact match |
| `postgresql_supabase: scp.public.permissions` | `application.supabase__scp__permissions` | 58 | 58 | 0 | PASS | 0 | 0 | 58 | exact match |
| `postgresql_supabase: scp.public.program_resource_mapping` | `application.supabase__scp__program_resource_mapping` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: scp.public.resources` | `application.supabase__scp__resources` | 16 | 16 | 0 | PASS | 0 | 0 | 16 | exact match |
| `postgresql_supabase: scp.public.review_stages` | `application.supabase__scp__review_stages` | 5 | 5 | 0 | PASS | 0 | 0 | 5 | exact match |
| `postgresql_supabase: scp.public.reviews` | `application.supabase__scp__reviews` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: scp.public.role_permission_mapping` | `application.supabase__scp__role_permission_mapping` | 197 | 197 | 0 | PASS | 0 | 0 | 197 | exact match |
| `postgresql_supabase: scp.public.rollouts` | `application.supabase__scp__rollouts` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_supabase: scp.public.sequelize_meta` | `application.supabase__scp__sequelize_meta` | 66 | 66 | 0 | PASS | 0 | 0 | 66 | exact match |
| `mongodb_elevate_atlas: crl_telemetry.telemetry` | `telemetry.elevate_events` | 11784 | 11784 | 0 | PASS | 0 | 0 | 11784 | exact match |
| `mongodb_elevate_atlas: elevate-diksha.telemetry` | `telemetry.elevate_events` | 216 | 216 | 0 | PASS | 0 | 0 | 216 | exact match |
| `mongodb_elevate_atlas: elevate-notification.telemetry` | `telemetry.elevate_events` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `mongodb_elevate_atlas: project.telemetry` | `telemetry.elevate_events` | 16 | 16 | 0 | PASS | 0 | 0 | 16 | exact match |
| `mongodb_elevate_atlas: samiksha.telemetry` | `telemetry.elevate_events` | 1868 | 1868 | 0 | PASS | 0 | 0 | 1868 | exact match |
| `postgresql_network_telemetry: global_registry.telemetry.events` | `telemetry.network_events` | 19597 | 19597 | 0 | PASS | 502 | 0 | 20099 | exact match |
| `postgresql_network_telemetry: global_registry.telemetry.ingestion_batches` | `telemetry.network_ingestion_batches` | 14877 | 14877 | 0 | PASS | 0 | 0 | 14877 | exact match |
| `postgresql_network_telemetry: global_registry.telemetry.rejected_events` | `telemetry.network_rejected_events` | 0 | 0 | 0 | PASS | 0 | 0 | 0 | exact match |
| `postgresql_network_telemetry: global_registry.telemetry.schema_migrations` | `telemetry.network_schema_migrations` | 3 | 3 | 0 | PASS | 0 | 0 | 3 | exact match |
| `postgresql_network_telemetry: global_registry.telemetry.services` | `telemetry.network_services` | 9 | 9 | 0 | FAIL | 0 | 7 | 16 | UNEXPLAINED: 1 source record(s) not live in target (absent or different content); UNEXPLAINED: 1 live target record(s) not in source (deletion not flagged or stale version) |

## Summary

- Total tables validated: 225
- Passed (live records match the source): 223
- Failed: 2
- Total source records: 76211
- Total live target records (for these objects): 76211
- Total record difference (source - live target): 0
- History kept: 76731 rows in total, 508 records flagged DELETED, 12 superseded versions
- Rejected records (source telemetry.rejected_events rows): 0
- Records failed during ingestion (audit): 0
- Source records not live in target: 2
- Unexplained discrepancies: 2 table(s)
- Registry entries with no discovered source: 0
- Duplicate versions / duplicate current versions in target tables: 0
- Target rows with null metadata / unknown ingestion_id: 0

## Audit checks

- Batches: 5891 total, 0 not SUCCESS, latest-batch-per-object not SUCCESS: 0
- audit.retired_records (legacy archive of source-purged rows, kept): 18
- error_logs rows: 4
- Unfinished runs: 0
- Runs whose inserted total != sum of job_execution: 0
- Jobs whose inserted total != sum of batches: 0
- Objects where sum(batch inserted) != ALL target rows: 0 []
- Objects where sum(new) - sum(flagged deleted) != live target records: 0 []
- Runs: RUN_20260930_140538_001=SUCCESS(failed tasks 0); RUN_20260930_141427_001=SUCCESS(failed tasks 0); RUN_20260930_141739_001=PARTIAL_FAILURE(failed tasks 1); RUN_20260930_141914_001=SUCCESS(failed tasks 0); RUN_20260930_142136_001=PARTIAL_FAILURE(failed tasks 1); RUN_20260930_145823_001=SUCCESS(failed tasks 0); RUN_20260930_151158_001=SUCCESS(failed tasks 0); RUN_20260930_151911_001=SUCCESS(failed tasks 0); RUN_20260930_154548_001=SUCCESS(failed tasks 0); RUN_20260930_155319_001=SUCCESS(failed tasks 0); RUN_20260930_160517_001=SUCCESS(failed tasks 0); RUN_20260930_160818_001=SUCCESS(failed tasks 0); RUN_20260930_162204_001=SUCCESS(failed tasks 0); RUN_20260930_162503_001=PARTIAL_FAILURE(failed tasks 2); RUN_20260930_162759_001=SUCCESS(failed tasks 0); RUN_20260930_163054_001=SUCCESS(failed tasks 0); RUN_20260930_163434_001=SUCCESS(failed tasks 0); RUN_20260930_164413_001=SUCCESS(failed tasks 0); RUN_20260930_164709_001=SUCCESS(failed tasks 0); RUN_20261001_043724_001=SUCCESS(failed tasks 0); RUN_20261001_103940_001=SUCCESS(failed tasks 0); RUN_20261001_130302_001=SUCCESS(failed tasks 0); RUN_20261001_131303_001=SUCCESS(failed tasks 0); RUN_20261001_162442_001=SUCCESS(failed tasks 0); RUN_20261001_162850_001=SUCCESS(failed tasks 0); RUN_20261001_170145_001=SUCCESS(failed tasks 0)