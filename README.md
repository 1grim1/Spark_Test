# parallel_destrib_lab3

curl —header "Content-Type: application/json" —request POST —data '{ "packageId":11, "jsScript":"var divideFn = function(a,b) { return a/b} ", "functionName":"divideFn", "tests": [ {"testName":"test1", "expectedResult":"2.0", "params":[2,1] },{"testName":"test2","expectedResult":"2.0","params":[4,2]}, {"testName":"test2","expectedResult":"8.0","params":[4,2]},{"testName":"test2","expectedResult":"8.0","params":[16,2]}] }' 127.0.0.1:8080


curl -X GET "127.0.0.1:8080/?packageId=11"
