# Define the directory containing the CSV files
$inputDirectory = "C:\temp\awards"  # Change this to your directory

# Define the output directory
$outputDirectory = Join-Path -Path $inputDirectory -ChildPath "out"

# Create the output directory if it does not exist
if (-not (Test-Path -Path $outputDirectory)) {
    New-Item -Path $outputDirectory -ItemType Directory
}

# Define the list of Product or Service Codes to filter
$codesToFilter = @("R499", "D399", "D306", "R408", "R410", "D308", "D318", "D301", "DC01", "DA01")

# Convert the codes to a HashSet for faster lookup
$codesHashSet = [System.Collections.Generic.HashSet[string]]::new()
$codesToFilter | ForEach-Object { $codesHashSet.Add($_) }

# Get all CSV files in the specified directory
$csvFiles = Get-ChildItem -Path $inputDirectory -Filter *.csv

# Loop through each CSV file
foreach ($csvFile in $csvFiles) {
    # Start timing the processing
    $startTime = Get-Date

    # Create the full path for the input file
    $inputFilePath = $csvFile.FullName
    
    # Generate the output filename by appending "_subset" to the input filename
    $outputFileName = [System.IO.Path]::GetFileNameWithoutExtension($csvFile.Name) + "_subset.csv"
    $outputFilePath = Join-Path -Path $outputDirectory -ChildPath $outputFileName

    # Initialize an array to hold filtered records
    $filteredData = @()

    # Initialize a counter for record numbers
    $recordCount = 0

    # Read the CSV file line-by-line using StreamReader
    $streamReader = [System.IO.StreamReader]::new($inputFilePath)

    # Read the header line
    $header = $streamReader.ReadLine()
    $columns = $header -split ','

    # Process the CSV file in batches
    while (-not $streamReader.EndOfStream) {
        $batchLines = @()

        # Read up to 1000 lines (adjust as necessary)
        for ($i = 0; $i -lt 1000 -and -not $streamReader.EndOfStream; $i++) {
            $line = $streamReader.ReadLine()
            $batchLines += $line
        }

        # Process each line in the batch
        foreach ($line in $batchLines) {
            $recordCount++

            # Split the line into values
            $values = $line -split ','

            # Create a custom object representing the record
            $record = @{}
            for ($j = 0; $j -lt $columns.Count; $j++) {
                $record[$columns[$j]] = $values[$j]
            }

            # Check if the product_or_service_code is in the HashSet
            if ($codesHashSet.Contains($record["product_or_service_code"])) {
                $filteredData += [PSCustomObject]$record
            }

            # Output the record number for every 1000 records processed
            if ($recordCount % 1000 -eq 0) {
                Write-Host "Processed $recordCount records from $($csvFile.Name)"
            }
        }
    }

    # Close the StreamReader
    $streamReader.Close()

    # Check if any records were found
    if ($filteredData.Count -eq 0) {
        Write-Host "No records found in $($csvFile.Name) matching the specified product_or_service_code values."
    } else {
        # Export the filtered records to a new CSV file
        $filteredData | Export-Csv -Path $outputFilePath -NoTypeInformation
        
        # Output the number of records saved
        Write-Host "Filtered records saved to: $outputFilePath"
        Write-Host "Number of records saved: $($filteredData.Count)"
    }

    # Stop timing the processing
    $endTime = Get-Date
    $duration = $endTime - $startTime

    # Output the processing time
    Write-Host "Processing time for $($csvFile.Name): $($duration.TotalSeconds) seconds"
}