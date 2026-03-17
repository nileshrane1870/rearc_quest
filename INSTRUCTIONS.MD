below command is for installation of analysis dpendencies for CDK
pip install --platform manylinux2014_x86_64 --target=..\analysis --implementation cp --python-version 3.9 --only-binary=:all: -r ..\analysis\requirements.txt

below commands are for installing deps in ingestion for CDK

go to lambda/ingestion

pip install -r requirements.txt -t .

if using cdk first got to cdk folder and run `cdk bootstrap`

if not using cdk first time in a project then go to cdk folder and run `cdk deploy` to deploy to cdk

------------------------------above is for cdk---------------------------------------------------

to run parts individually run `pip install -r requirements.txt` at root then `python part1.py` for part 1, then `python part2.py` for part 2, then `python part3_analysis.py` for part 3.