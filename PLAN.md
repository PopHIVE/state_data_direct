Here's the plan summary:

20 states failing -- 18 use identical generic chromote boilerplate that doesn't work. The fix: identify each state's actual dashboard platform and call its API directly, just like AZ does with Power BI.

Phase 1 (5 quick wins):

NC -- "data-behind-dashboards" page has CSV links → direct download pattern
OH -- data.ohio.gov is Socrata → PA template
IL -- public.data.illinois.gov is Socrata → PA template (29 indicators, biggest payoff)
MD -- Tableau Public → OR template
KY -- Tableau Server → OR template (adapted)
Phase 2 (13 states): Require browser DevTools inspection to identify platforms (WI, IA, OK, NJ, SC, IN, MA, UT, FL, AK, GA, MO, AL), then apply the matching template.

Phase 3 (2 states): MI and TN are almost working but download wrong files -- just need URL filtering fixes.

Each rewrite replaces ~200 lines of chromote boilerplate with ~30-80 lines of direct API calls, keeping the standard DCF header/footer.