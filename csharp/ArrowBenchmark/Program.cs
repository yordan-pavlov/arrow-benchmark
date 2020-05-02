// #define LOG
using Apache.Arrow;
using Apache.Arrow.Memory;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Running;
using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;

namespace ArrowBenchmark
{
    // to run this benchmark, navigate up to the csharp directory and execute command:
    // dotnet run -p ArrowBenchmark -c Release
    [SimpleJob(launchCount: 1, invocationCount: 1, warmupCount: 5, targetCount: 20)]
    public class FilterBenchmark
    {
        private List<LandRegistryRecord> landRegistryRecords;
        private RecordBatch recordBatch;

        [GlobalSetup]
        public void BenchmarkSetup()
        {
            // load csv file into list of objects 
            string landRegistryDataPath = string.Concat(Enumerable.Repeat("..\\", 9)) + "data\\pp-monthly-update-new-version.csv";
            
            Console.WriteLine("Loading land registry data");
            var dataLoadTime = Stopwatch.StartNew();
            using (var reader = new StreamReader(landRegistryDataPath))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                // land registry data file does not have headers
                csv.Configuration.HasHeaderRecord = false;
                csv.Configuration.RegisterClassMap<LandRegistryRecordMap>();
                this.landRegistryRecords = csv.GetRecords<LandRegistryRecord>().ToList();
            }
            dataLoadTime.Stop();
            Console.WriteLine("Loading CSV data took {0}ms", dataLoadTime.ElapsedMilliseconds);
            Console.WriteLine("Loaded {0} records", landRegistryRecords.Count);
            Console.WriteLine("----------------");

            // load csv file into apache arrow arrays / table
            Console.WriteLine("Loading land registry records in arrow arrays");
            var stringEncoding = Encoding.ASCII;
            var arrowLoadTime = Stopwatch.StartNew();
            var memoryAllocator = new NativeMemoryAllocator(alignment: 64);
            this.recordBatch = new RecordBatch.Builder(memoryAllocator)
                .Append("Date", false, col => col.Date32(array => array.AppendRange(landRegistryRecords.Select(r => r.Date))))
                .Append("Price", false, col => col.Float(array => array.AppendRange(landRegistryRecords.Select(r => r.Price))))
                .Append("PropertyType", false, col => col.String(array => array.AppendRange(landRegistryRecords.Select(r => r.PropertyType), stringEncoding)))
                .Append("Tenure", false, col => col.String(array => array.AppendRange(landRegistryRecords.Select(r => r.Tenure), stringEncoding)))
                .Build();
            arrowLoadTime.Stop();
            Console.WriteLine("Loaded {0} arrays with length {1} in {2}ms", recordBatch.ColumnCount, recordBatch.Length, arrowLoadTime.ElapsedMilliseconds);
            Console.WriteLine("----------------");
        }

        [Benchmark]
        public void FilterLandRegistryRecordsListLoop()
        {
            //var filterPredicate = BuildFilterPredicate();
            var dateFilter = DateTimeOffset.Parse("2019-01-01");
            var propertyTypeFilter = new string[] { "D", "S", "T" };
            var result = 0;
            var itemCount = this.landRegistryRecords.Count;
            for (var i = 0; i < itemCount; i++)
            {
                var r = this.landRegistryRecords[i];
                if (r.Date >= dateFilter && r.Price > 5000000 && propertyTypeFilter.Contains(r.PropertyType) && r.Tenure == "F")
                {
                    result++;
                }
            }
#if LOG
            Console.WriteLine("Found {0} records", result);
#endif
        }

        [Benchmark]
        public int FilterLandRegistryRecordsListLinq()
        {
            var dateFilter = DateTimeOffset.Parse("2019-01-01");
            var propertyTypeFilter = new string[] { "D", "S", "T" };
            var itemCount =  this.landRegistryRecords
                .Where(r => r.Date >= dateFilter && r.Price > 5000000 && propertyTypeFilter.Contains(r.PropertyType) && r.Tenure == "F")
                .Count();
#if LOG
            Console.WriteLine("Found {0} records", itemCount);
#endif
            return itemCount;
        }

        [Benchmark]
        public int FilterLandRegistryRecordsArrow()
        {
            var recordCount = recordBatch.Length;
            var selectMask = new bool[recordCount];

            const long MillisecondsPerDay = 86400000;

            var dateFilter = (int)(DateTimeOffset.Parse("2019-01-01").ToUnixTimeMilliseconds() / MillisecondsPerDay);
            var dateValues = (recordBatch.Column(0) as Date32Array).Values;
            for (var i = 0; i < recordCount; i++)
            {
                selectMask[i] = dateValues[i] >= dateFilter;
            }

            var priceValues = (recordBatch.Column(1) as FloatArray).Values;
            for (var i = 0; i < recordCount; i++)
            {
                selectMask[i] = selectMask[i] && priceValues[i] > 5000000;
            }

            var stringEncoding = Encoding.ASCII;
            var propertyTypeFilter = new string[] { "D", "S", "T" }.Select(x => stringEncoding.GetBytes(x)[0]).ToArray();
            var propertyTypeValues = (recordBatch.Column(2) as StringArray).Values;
            for (var i = 0; i < recordCount; i++)
            {
                selectMask[i] = selectMask[i] && propertyTypeFilter.Contains(propertyTypeValues[i]);
            }

            var tenureFilter = stringEncoding.GetBytes("F")[0];
            var tenureValues = (recordBatch.Column(3) as StringArray).Values;
            for (var i = 0; i < recordCount; i++)
            {
                selectMask[i] = selectMask[i] && tenureValues[i] == tenureFilter;
            }

            var itemCount = selectMask.Count(v => v);
#if LOG
            Console.WriteLine("Found {0} records", itemCount);
#endif
            return itemCount;
        }

        [Benchmark]
        public int FilterLandRegistryRecordsArrowVectorized()
        {
            var recordCount = recordBatch.Length;
            var selectMask = new int[recordCount]; // new int[0];

            const long MillisecondsPerDay = 86400000;

            var dateFilter = (int)(DateTimeOffset.Parse("2019-01-01").ToUnixTimeMilliseconds() / MillisecondsPerDay);
            var dateFilterVector = new Vector<int>(dateFilter);
            var dateFilterVectorLimit = recordCount - (recordCount % Vector<int>.Count);
            var dateValues = (recordBatch.Column(0) as Date32Array).Values;
            for (var i = 0; i < dateFilterVectorLimit; i += Vector<int>.Count)
            {
                var resultVector = Vector.GreaterThanOrEqual(
                    new Vector<int>(dateValues.Slice(i)),
                    dateFilterVector
                );
                resultVector.CopyTo(selectMask, i);
            }
            for (var i = dateFilterVectorLimit; i < recordCount; i++)
            {
                var predicateResult = dateValues[i] >= dateFilter;
                selectMask[i] = Unsafe.As<bool, int>(ref predicateResult);
                //selectMask[i] = dateValues[i] >= dateFilter ? -1 : 0;
            }

            //selectMask = GetFilterMask<int>(dateValues, Vector.GreaterThanOrEqual, (v, f) => v >= f, dateFilter);

            var priceFilterVector = new Vector<float>(5000000);
            var priceFilterVectorLimit = recordCount - (recordCount % Vector<float>.Count);
            var priceValues = (recordBatch.Column(1) as FloatArray).Values;
            for (var i = 0; i < priceFilterVectorLimit; i += Vector<float>.Count)
            {
                Vector.BitwiseAnd(
                    new Vector<int>(selectMask, i),
                    Vector.GreaterThan(
                        new Vector<float>(priceValues.Slice(i)),
                        priceFilterVector
                    )
                ).CopyTo(selectMask, i);
            }
            for (var i = priceFilterVectorLimit; i < recordCount; i++)
            {
                var predicateResult = priceValues[i] > 5000000;
                selectMask[i] &= Unsafe.As<bool, int>(ref predicateResult);
                //selectMask[i] = selectMask[i] && priceValues[i] > 5000000 ? -1 : 0;
            }

            byte[] byteSelectMask = VectorizedFiltering.NarrowFilterMask(selectMask);

            var stringEncoding = Encoding.ASCII;
            var propertyTypeSelectMask = new byte[byteSelectMask.Length];
            var propertyTypeFilterList = new string[] { "D", "S", "T" }.Select(x => stringEncoding.GetBytes(x)[0]).ToArray();
            var propertyTypeFilterVectorLimit = recordCount - (recordCount % Vector<byte>.Count);
            for (var j = 0; j < propertyTypeFilterList.Length; j++)
            {
                var propertyTypeFilter = propertyTypeFilterList[j];
                var propertyTypeFilterVector = new Vector<byte>(propertyTypeFilter);
                var propertyTypeValues = (recordBatch.Column(2) as StringArray).Values;
                for (var i = 0; i < propertyTypeFilterVectorLimit; i += Vector<byte>.Count)
                {
                    Vector.BitwiseOr(
                        new Vector<byte>(propertyTypeSelectMask, i),
                        Vector.GreaterThan(
                            new Vector<byte>(propertyTypeValues.Slice(i)),
                            propertyTypeFilterVector
                        )
                    ).CopyTo(propertyTypeSelectMask, i);
                }
                for (var i = propertyTypeFilterVectorLimit; i < recordCount; i++)
                {
                    var predicateResult = propertyTypeValues[i] == propertyTypeFilter;
                    propertyTypeSelectMask[i] |= Unsafe.As<bool, byte>(ref predicateResult);
                    //byteSelectMask[i] = tenureValues[i] == tenureFilter ? -1 : 0;
                }
                //for (var i = 0; i < recordCount; i++)
                //{
                //    selectMask[i] = selectMask[i] && propertyTypeFilter.Contains(propertyTypeValues[i]);
                //}
            }

            for (var i = 0; i < propertyTypeFilterVectorLimit; i += Vector<byte>.Count)
            {
                Vector.BitwiseAnd(
                    new Vector<byte>(byteSelectMask, i),
                    new Vector<byte>(propertyTypeSelectMask, i)
                ).CopyTo(byteSelectMask, i);
            }
            for (var i = propertyTypeFilterVectorLimit; i < recordCount; i++)
            {
                byteSelectMask[i] &= propertyTypeSelectMask[i];
            }

            var tenureFilter = stringEncoding.GetBytes("F")[0];
            var tenureFilterVector = new Vector<byte>(tenureFilter);
            var tenureFilterVectorLimit = recordCount - (recordCount % Vector<byte>.Count);
            var tenureValues = (recordBatch.Column(3) as StringArray).Values;
            for (var i = 0; i < tenureFilterVectorLimit; i += Vector<byte>.Count)
            {
                Vector.BitwiseAnd(
                    new Vector<byte>(byteSelectMask, i),
                    Vector.GreaterThan(
                        new Vector<byte>(tenureValues.Slice(i)),
                        tenureFilterVector
                    )
                ).CopyTo(byteSelectMask, i);
            }
            for (var i = tenureFilterVectorLimit; i < recordCount; i++)
            {
                var predicateResult = tenureValues[i] == tenureFilter;
                byteSelectMask[i] &= Unsafe.As<bool, byte>(ref predicateResult);
                //byteSelectMask[i] = byteSelectMask[i] && tenureValues[i] == tenureFilter ? -1 : 0;
            }

            var countVector = Vector<byte>.Zero;
            for (var i = 0; i < tenureFilterVectorLimit; i += Vector<byte>.Count)
            {
                countVector += new Vector<byte>(byteSelectMask, i) & Vector<byte>.One;
            }
            var count = 0;
            for (var i = 0; i < Vector<byte>.Count; i++)
            {
                count += countVector[i];
            }
            for (var i = tenureFilterVectorLimit; i < recordCount; i++)
            {
                count += byteSelectMask[i] & 1;
            }
#if LOG
            Console.WriteLine("Found {0} records", count);
#endif
            return count;
            //return selectMask.Count(v => v == -1);
        }

        // using AVX2 instructions directly requires compiling with unafe
        [Benchmark]
        public unsafe int FilterLandRegistryRecordsArrowVectorizedAvx()
        {
            var recordCount = recordBatch.Length;
            var selectMask = new int[recordCount]; // new int[0];

            const long MillisecondsPerDay = 86400000;

            var dateFilter = (int)(DateTimeOffset.Parse("2019-01-01").ToUnixTimeMilliseconds() / MillisecondsPerDay);
            var dateFilterVector = Vector256.Create(dateFilter);
            var dateFilterVectorLimit = recordCount - (recordCount % Vector256<int>.Count);
            var dateValues = (recordBatch.Column(0) as Date32Array).Values;
            fixed (int* pDateValues = dateValues, pSelectMask = selectMask)
            {
                for (var i = 0; i < dateFilterVectorLimit; i += Vector256<int>.Count)
                {
                    
                    var resultVector = Avx2.CompareGreaterThan(
                        Avx2.LoadVector256(pDateValues + i),
                        dateFilterVector
                    );
                    Avx2.Store(pSelectMask + i, resultVector);
                }
            }
            for (var i = dateFilterVectorLimit; i < recordCount; i++)
            {
                var predicateResult = dateValues[i] >= dateFilter;
                selectMask[i] = Unsafe.As<bool, int>(ref predicateResult);
                //selectMask[i] = dateValues[i] >= dateFilter ? -1 : 0;
            }

            var priceFilterVector = Vector256.Create(5000000f).AsInt32();
            var priceFilterVectorLimit = recordCount - (recordCount % Vector256<float>.Count);
            var priceValues = (recordBatch.Column(1) as FloatArray).Values;
            fixed (float* pPriceValues = priceValues)
            fixed (int* pSelectMask = selectMask)
            {
                for (var i = 0; i < priceFilterVectorLimit; i += Vector256<float>.Count)
                {
                    var resultVector = Avx2.And(
                        Avx2.LoadVector256(pSelectMask + i),
                        Avx2.CompareGreaterThan(
                            Avx2.LoadVector256(pPriceValues + i).AsInt32(),
                            priceFilterVector
                        )
                    );
                    Avx2.Store(pSelectMask + i, resultVector);
                }
            }
            for (var i = priceFilterVectorLimit; i < recordCount; i++)
            {
                var predicateResult = priceValues[i] > 5000000;
                selectMask[i] &= Unsafe.As<bool, int>(ref predicateResult);
                //selectMask[i] = selectMask[i] && priceValues[i] > 5000000 ? -1 : 0;
            }

            byte[] byteSelectMask = VectorizedFiltering.NarrowFilterMask(selectMask);

            var stringEncoding = Encoding.ASCII;
            var propertyTypeSelectMask = new byte[byteSelectMask.Length];
            var propertyTypeFilterList = new string[] { "D", "S", "T" }.Select(x => stringEncoding.GetBytes(x)[0]).ToArray();
            var propertyTypeFilterVectorLimit = recordCount - (recordCount % Vector256<byte>.Count);
            for (var j = 0; j < propertyTypeFilterList.Length; j++)
            {
                var propertyTypeFilter = propertyTypeFilterList[j];
                var propertyTypeFilterVector = Vector256.Create(propertyTypeFilter).AsSByte();
                var propertyTypeValues = (recordBatch.Column(2) as StringArray).Values;
                fixed (byte* pPropertyTypeValues = propertyTypeValues, pPropertyTypeSelectMask = propertyTypeSelectMask)
                {
                    for (var i = 0; i < propertyTypeFilterVectorLimit; i += Vector256<byte>.Count)
                    {
                        var resultVector = Avx2.And(
                            Avx2.LoadVector256(pPropertyTypeSelectMask + i),
                            Avx2.CompareGreaterThan(
                                Avx2.LoadVector256(pPropertyTypeValues + i).AsSByte(),
                                propertyTypeFilterVector
                            ).AsByte()
                        );
                        Avx2.Store(pPropertyTypeSelectMask + i, resultVector);
                    }
                }
                for (var i = propertyTypeFilterVectorLimit; i < recordCount; i++)
                {
                    var predicateResult = propertyTypeValues[i] == propertyTypeFilter;
                    propertyTypeSelectMask[i] |= Unsafe.As<bool, byte>(ref predicateResult);
                    //byteSelectMask[i] = tenureValues[i] == tenureFilter ? -1 : 0;
                }
                //for (var i = 0; i < recordCount; i++)
                //{
                //    selectMask[i] = selectMask[i] && propertyTypeFilter.Contains(propertyTypeValues[i]);
                //}
            }

            fixed (byte* pByteSelectMask = byteSelectMask, pPropertyTypeSelectMask = propertyTypeSelectMask)
            {
                for (var i = 0; i < propertyTypeFilterVectorLimit; i += Vector256<byte>.Count)
                {
                    var resultVector = Avx2.And(
                        Avx2.LoadVector256(pByteSelectMask + i),
                        Avx2.LoadVector256(pPropertyTypeSelectMask + i)
                    );
                    Avx2.Store(pByteSelectMask + i, resultVector);
                }
            }
            for (var i = propertyTypeFilterVectorLimit; i < recordCount; i++)
            {
                byteSelectMask[i] &= propertyTypeSelectMask[i];
            }

            var tenureFilter = stringEncoding.GetBytes("F")[0];
            var tenureFilterVector = Vector256.Create(tenureFilter).AsSByte();
            var tenureFilterVectorLimit = recordCount - (recordCount % Vector256<byte>.Count);
            var tenureValues = (recordBatch.Column(3) as StringArray).Values;
            fixed (byte* pByteSelectMask = byteSelectMask, pTenureValues = tenureValues)
            {
                for (var i = 0; i < tenureFilterVectorLimit; i += Vector256<byte>.Count)
                {
                    var resultVector = Avx2.And(
                        Avx2.LoadVector256(pByteSelectMask + i),
                        Avx2.CompareGreaterThan(
                            Avx2.LoadVector256(pTenureValues + i).AsSByte(),
                            tenureFilterVector
                        ).AsByte()
                    );
                    Avx2.Store(pByteSelectMask + i, resultVector);
                }
            }
            for (var i = tenureFilterVectorLimit; i < recordCount; i++)
            {
                var predicateResult = tenureValues[i] == tenureFilter;
                byteSelectMask[i] &= Unsafe.As<bool, byte>(ref predicateResult);
                //byteSelectMask[i] = byteSelectMask[i] && tenureValues[i] == tenureFilter ? -1 : 0;
            }

            var countVector = Vector256<byte>.Zero;
            var oneVector = Vector256.Create((byte)1);
            fixed (byte* pByteSelectMask = byteSelectMask, pTenureValues = tenureValues)
            {
                for (var i = 0; i < tenureFilterVectorLimit; i += Vector256<byte>.Count)
                {
                    countVector = Avx2.Add(
                        countVector,
                        Avx2.And(Avx2.LoadVector256(pByteSelectMask + i), oneVector)
                    );
                }
            }
            var count = 0;
            for (var i = 0; i < Vector256<byte>.Count; i++)
            {
                count += countVector.GetElement(i);
            }
            for (var i = tenureFilterVectorLimit; i < recordCount; i++)
            {
                count += byteSelectMask[i] & 1;
            }
#if LOG
            Console.WriteLine("Found {0} records", count);
#endif
            return count;
            //return selectMask.Count(v => v == -1);
        }

        [Benchmark]
        public int FilterLandRegistryRecordsArrowVectorized2()
        {
            var recordCount = recordBatch.Length;
            //var selectMask = new byte[recordCount]; // new int[0];
            int[] selectMask = new int[recordCount];
            const long MillisecondsPerDay = 86400000;
            var dateFilter = (int)(DateTimeOffset.Parse("2019-01-01").ToUnixTimeMilliseconds() / MillisecondsPerDay);
            var dateValues = (recordBatch.Column(0) as Date32Array).Values;
            VectorizedFiltering.ApplyFilterMask<int, int>(
                dateValues, Vector.GreaterThanOrEqual, (v, f) => v >= f, dateFilter,
                selectMask, Vector.BitwiseOr, (f1, f2) => (byte)(f1 | f2));

            var priceValues = (recordBatch.Column(1) as FloatArray).Values;
            VectorizedFiltering.ApplyFilterMask<float, int>(
                priceValues, Vector.GreaterThan, (v, f) => v > f, 5000000,
                selectMask, Vector.BitwiseAnd, (f1, f2) => f1 & f2);

            byte[] byteSelectMask = VectorizedFiltering.NarrowFilterMask(selectMask);
            var stringEncoding = Encoding.ASCII;
            var propertyTypeFilter = new string[] { "D", "S", "T" }.Select(x => stringEncoding.GetBytes(x)[0]).ToArray();
            var propertyTypeValues = (recordBatch.Column(2) as StringArray).Values;
            byte[] byteSelectMask2 = VectorizedFiltering.GetFilterMask<byte>(
                propertyTypeValues, Vector.Equals, (v, f) => v == f, propertyTypeFilter);
            byteSelectMask = VectorizedFiltering.CombineFilterMask(
                byteSelectMask, byteSelectMask2, Vector.BitwiseAnd, (f1, f2) => (byte)(f1 & f2));

            var tenureFilter = stringEncoding.GetBytes("F")[0];
            var tenureValues = (recordBatch.Column(3) as StringArray).Values;
            VectorizedFiltering.ApplyFilterMask<byte, byte>(
                tenureValues, Vector.Equals, (v, f) => v == f, tenureFilter,
                byteSelectMask, Vector.BitwiseAnd, (f1, f2) => (byte)(f1 & f2));

            var count = VectorizedFiltering.CountFilterMask(byteSelectMask);

#if LOG
            Console.WriteLine("Found {0} records", count);
#endif
            return count;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            // counting of items selected in a filter mask could be done using vector operations
            // by dividing the elements of the orginal vector into two vectors and then adding
            // and doing this until a single element remains containing the count:
            //var one = Vector256.Create((byte)1);
            //var firstHalf = Avx2.ExtractVector128(one, 0);
            //var secondHalf = Avx2.ExtractVector128(one, (byte)(Vector256<byte>.Count / 2));
            //Avx.Add(firstHalf, secondHalf);

            //var filterBenchmark = new FilterBenchmark();
            //filterBenchmark.BenchmarkSetup();
            //int itemCount = filterBenchmark.FilterLandRegistryRecordsArrowVectorized2();

            //Console.WriteLine("IsHardwareAccelerated: {0} ({1})", Vector.IsHardwareAccelerated, Vector<int>.One);
            //var greaterVector = Vector.GreaterThanOrEqual(Vector<int>.One, Vector<int>.Zero);
            //Console.WriteLine("greaterVector: {0}", greaterVector);

            // new DebugInProcessConfig()
            var summary = BenchmarkRunner.Run<FilterBenchmark>();
        }
    }

    public static class VectorizedFiltering
    {
        public static T[] CombineFilterMask<T>(
            T[] filterMask1, T[] filterMask2, 
            Func<Vector<T>, Vector<T>, Vector<T>> combineVectorOp,
            Func<T, T, T> combineItemOp
        ) where T: unmanaged
        {
            int itemCount = filterMask1.Length;
            int vectorSize = Vector<T>.Count;
            var lastVectorIndex = itemCount - (itemCount % vectorSize);
            for (var i = 0; i < lastVectorIndex; i += vectorSize)
            {
                // Vector.BitwiseAnd
                combineVectorOp(
                    new Vector<T>(filterMask1, i),
                    new Vector<T>(filterMask2, i)
                ).CopyTo(filterMask1, i);
            }
            for (var i = lastVectorIndex; i < itemCount; i++)
            {
                //filterMask1[i] &= filterMask2[i];
                filterMask1[i] = combineItemOp(filterMask1[i], filterMask2[i]);
            }
            return filterMask1;
        }

        public static int CountFilterMask(byte[] filterMask)
        {
            var itemCount = filterMask.Length;
            var vectorSize = Vector<byte>.Count;
            var lastVectorLimit = itemCount - (itemCount % vectorSize);
            var countVector = Vector<byte>.Zero;
            for (var i = 0; i < lastVectorLimit; i += vectorSize)
            {
                //TODO: use Avx2.HorizontalAdd()
                countVector += new Vector<byte>(filterMask, i) & Vector<byte>.One;
            }
            var count = 0;
            for (var i = 0; i < vectorSize; i++)
            {
                count += countVector[i];
            }
            for (var i = lastVectorLimit; i < itemCount; i++)
            {
                count += filterMask[i] & 1;
            }
            return count;
        }

        public static byte[] GetFilterMask<T>(
            ReadOnlySpan<T> values,
            Func<Vector<T>, Vector<T>, Vector<byte>> vectorOp,
            Func<T, T, bool> filterOp,
            T[] filterValues
        ) where T : struct
        {
            byte[] filterMask = new byte[values.Length];
            for (var i = 0; i < filterValues.Length; i++)
            {
                ApplyFilterMask<T, byte>(
                    values, vectorOp, filterOp, filterValues[i], 
                    filterMask, Vector.BitwiseOr, (f1, f2) => (byte)(f1 | f2)
                );
            }
            return filterMask;
        }

        public static void ApplyFilterMask<T, M>(
            ReadOnlySpan<T> values,
            Func<Vector<T>, Vector<T>, Vector<M>> vectorOp,
            Func<T, T, bool> filterOp,
            T filterValue,
            M[] filterMask,
            Func<Vector<M>, Vector<M>, Vector<M>> combineVectorOp,
            Func<M, M, M> combineItemOp
        ) where T : struct where M : unmanaged
        {
            var recordCount = filterMask.Length;
            var vectorSize = Vector<T>.Count;
            var lastVectorIndex = recordCount - (recordCount % vectorSize);
            var filterVector = new Vector<T>(filterValue);
            for (int i = 0; i < lastVectorIndex; i += vectorSize)
            {
                combineVectorOp(
                    new Vector<M>(filterMask, i),
                    vectorOp(
                        new Vector<T>(values.Slice(i)),
                        filterVector
                    )
                ).CopyTo(filterMask, i);
            }
            //var zeroValue = default(M);
            //var oneValue = default(M);
            //Unsafe.InitBlock(ref Unsafe.As<M, byte>(ref oneValue), 255, (uint)Unsafe.SizeOf<M>());
            for (var i = lastVectorIndex; i < recordCount; i++)
            {
                var predicateResult = filterOp(values[i], filterValue);
                filterMask[i] = combineItemOp(filterMask[i], Unsafe.As<bool, M>(ref predicateResult));
                //filterMask[i] = filterOp(values[i], filterValue) ? oneValue : zeroValue;
            }
        }

        public static byte[] NarrowFilterMask(int[] filterMask)
        {
            var recordCount = filterMask.Length;
            var narrowedFilterMask = new byte[recordCount];
            var vectorSize = Vector<byte>.Count;
            var intVectorSize = Vector<int>.Count;
            var lastVectorIndex = recordCount - (recordCount % vectorSize);
            for (int i = 0; i < lastVectorIndex; i += vectorSize)
            {
                var narrowedFilterSpan = new Span<byte>(narrowedFilterMask, i, vectorSize);
                Vector.Narrow(
                    Vector.Narrow(new Vector<int>(filterMask, i), new Vector<int>(filterMask, i + intVectorSize)),
                    Vector.Narrow(new Vector<int>(filterMask, i + (intVectorSize * 2)), new Vector<int>(filterMask, i + (intVectorSize * 3)))
                ).CopyTo(narrowedFilterSpan);
            }
            for (int i = lastVectorIndex; i < recordCount; i++)
            {
                narrowedFilterMask[i] = (byte)filterMask[i];
            }
            return narrowedFilterMask;
        }

        private static void CopyResultVectorsToFilterMask(Vector<int>[] resultVectorList, Span<short> filterMask)
        {
            var remainderVectorCount = resultVectorList.Length % 2;
            var lastVectorGroupIndex = resultVectorList.Length - remainderVectorCount;
            for (var i = 0; i < lastVectorGroupIndex; i += 2)
            {
                //Avx2.LoadVector256()
                //Avx2.PackSignedSaturate()
                //Vector.Narrow(
                //    Vector.Narrow(resultVectorList[i], resultVectorList[i + 1]),
                //    Vector.Narrow(resultVectorList[i + 2], resultVectorList[i + 3])
                //).CopyTo(filterMask.Slice(i * Vector<int>.Count));
                Vector
                    .Narrow(resultVectorList[i], resultVectorList[i + 1])
                    .CopyTo(filterMask.Slice(i * Vector<int>.Count));
            }
            if (remainderVectorCount == 0)
            {
                return;
            }
            var remainderVectors = new Vector<int>[2];
            int j = 0;
            for (var i = lastVectorGroupIndex; i < resultVectorList.Length; i++)
            {
                remainderVectors[j++] = resultVectorList[i];
            }
            for (; j < 2; j++)
            {
                remainderVectors[j] = Vector<int>.Zero;
            }
            //var narrowedVector = Vector.Narrow(
            //        Vector.Narrow(remainderVectors[0], remainderVectors[1]),
            //        Vector.Narrow(remainderVectors[2], remainderVectors[3]));
            var narrowedVector = Vector.Narrow(remainderVectors[0], remainderVectors[1]);
            var tempByteArray = new short[Vector<short>.Count];
            narrowedVector.CopyTo(tempByteArray);
            tempByteArray
                .AsSpan(0, Vector<int>.Count * remainderVectorCount)
                .CopyTo(filterMask.Slice(lastVectorGroupIndex * Vector<int>.Count));

            //var narrowedVector = Vector.Narrow(
            //    Vector.Narrow(new Vector<int>(-1), Vector<int>.Zero),
            //    Vector.Narrow(Vector<int>.Zero, Vector<int>.Zero));
            //Console.WriteLine("narrowed vector: {0}", narrowedVector);
        }
    }

    public static class EnumerableExtensions
    {
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ForEach<T>(this IEnumerable<T> source, Action<T> action)
        {
            foreach (T item in source)
            {
                action(item);
            }
        }
    }

    // land registry csv file model
    public class LandRegistryRecord
    {
        public float Price { get; set; }
        public DateTimeOffset Date { get; set; }
        public string PostCode { get; set; }
        public string PropertyType { get; set; }
        public string IsNew { get; set; }
        public string Tenure { get; set; }
        public string PrimaryName { get; set; }
        public string SecondaryName { get; set; }
        public string Street { get; set; }
        public string City { get; set; }
        //public string Category { get; set; }
        public string RecordType { get; set; }
    }

    public class LandRegistryRecordMap : ClassMap<LandRegistryRecord>
    {
        public LandRegistryRecordMap()
        {
            //price: String::from(&record[1]).trim().to_uppercase()
            Map(m => m.Price).Index(1);
            //date: String::from(&record[2].trim_start()[..10]),
            Map(m => m.Date).Index(2).ConvertUsing(reader => DateTimeOffset.Parse(reader.GetField(2).Substring(0, 10)));
            //post_code: String::from(&record[3]).trim().to_uppercase(),
            Map(m => m.PostCode).Index(3);
            //property_type: LandRegistryRecord::deserialise_property_type(&record[4]),
            Map(m => m.PropertyType).Index(4);
            //is_new: String::from(&record[5]).trim().to_uppercase(),
            Map(m => m.IsNew).Index(5);
            //tenure: LandRegistryRecord::deserialise_tenure(&record[6]),
            Map(m => m.Tenure).Index(6);
            //primary_name: String::from(&record[7]).trim().to_uppercase(),
            Map(m => m.PrimaryName).Index(7);
            //secondary_name: String::from(&record[8]).trim().to_uppercase(),
            Map(m => m.SecondaryName).Index(8);
            //street: String::from(&record[9]).trim().to_uppercase(),
            Map(m => m.Street).Index(9);
            //// skip locality field
            //city: String::from(&record[11]).trim().to_uppercase(),
            Map(m => m.City).Index(11);
            //// skip district field
            //// skip county field,
            ////category: String::from(&record[14]).trim().to_uppercase(),
            //record_type: String::from(&record[15]).trim().to_uppercase()
            Map(m => m.RecordType).Index(15);
        }
    }
}
