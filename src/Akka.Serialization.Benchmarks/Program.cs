//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Actor.Setup;
using Akka.Configuration;
using Akka.Serialization;
using Akka.Serialization.MessagePack;
using Akka.Util;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using Newtonsoft.Json;

namespace SerializationBenchmarks
{
    public class SimpleActor : ReceiveActor
    {
        public SimpleActor()
        {
            
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            //BenchmarkRunner.Run<MessagePackSerializerTests>();
            BenchmarkSwitcher.FromAssemblies(new[] { typeof(Program).Assembly })
                .RunAllJoined();
        }
    }

    public class TestSer
    {
        public int Id { get; set; }
        public string someStr { get; set; }
        public string someStr2 { get; set; }
        public string someStr3 { get; set; }
        public Guid IDK { get; set; }
    }

    internal static class HelperExts
    {
        public static string GetManifest(this Serializer ser, object obj)
        {
            if (ser is SerializerWithStringManifest swsm)
            {
                return swsm.Manifest(obj);
            }
            else
            {
                return TypeExtensions.TypeQualifiedName(obj.GetType());
;            }
        }
    }
    public class TestSer_WithRef
    {
        public int Id { get; set; }
        public string someStr { get; set; }
        public string someStr2 { get; set; }
        public string someStr3 { get; set; }
        public Guid IDK { get; set; }
        public IActorRef Ref { get; set; }
    }

    public class TestSer_Inheriting : TestSer
    {
        public string NewProp { get; set; }
    }

    public sealed class TestSer_Sealed
    {
        public string SomeValue { get; set; }
    }

    public class TestSer_Enclosed_Polymorphic_TestSerNoRef
    {
        public TestSer SomeValue { get; set; }
    }
    public class TestSer_Enclosed_Polymorphic_Object
    {
        public object Value { get; set; }
    }

    public class JsonSerializerTests : BaseSerializerTests
    {
        protected override string testSysName => "bench-serialization-json-nopool";

        protected override Func<ExtendedActorSystem, ImmutableHashSet<SerializerDetails>>
            createSerializer => (eas) =>
            ImmutableHashSet.Create( SerializerDetails.Create("json", new NewtonSoftJsonSerializer(eas),
                ImmutableHashSet.Create(typeof(object))));
    }
    
    public class HyperionSerializerTests : BaseSerializerTests
    {
        protected override string testSysName => "bench-serialization-hyperion";

        protected override Func<ExtendedActorSystem, ImmutableHashSet<SerializerDetails>>
            createSerializer => (eas) =>
            ImmutableHashSet.Create( SerializerDetails.Create("hyperion", new HyperionSerializer(eas),
                ImmutableHashSet.Create(typeof(object))));
    }
    
    public class MessagePackSerializerTests : BaseSerializerTests
    {
        protected override string testSysName => "bench-serialization-messagepack";

        protected override Func<ExtendedActorSystem, ImmutableHashSet<SerializerDetails>>
            createSerializer => (eas) =>
            ImmutableHashSet.Create( SerializerDetails.Create("hyperion", new MsgPackSerializer(eas),
                ImmutableHashSet.Create(typeof(object))));
    }
    
    [MemoryDiagnoser]
    public abstract class BaseSerializerTests
    {
        protected BaseSerializerTests()
        {
            var setup = BootstrapSetup.Create().And(
                SerializationSetup.Create(createSerializer));
            _sys_noPool = ActorSystem.Create(testSysName, setup);
            _poolSer = 
            _sys_noPool.Serialization.FindSerializerForType(typeof(object));
            var aRef = _sys_noPool.ActorOf<SimpleActor>();
            testObj_WithRef =  new()
            {
                Id = 124,
                someStr =
                    "412tgieoargj4a9349u2u-03jf3290rjf2390ja209fj1099u42n0f92qm93df3m-032jfq-102",
                someStr2 =
                    "412tgieoargj4a9349u2u-03jf3290rjf2390ja209fj1099u42n0f92qm93df3m-032jfq-102",
                someStr3 =
                    new string(Enumerable.Repeat('l',512).ToArray()),
                IDK = Guid.Empty, Ref = aRef
            };
            _testObj_inheriting = new TestSer_Inheriting()
            {
                Id = 124,
                someStr =
                    "412tgieoargj4a9349u2u-03jf3290rjf2390ja209fj1099u42n0f92qm93df3m-032jfq-102",
                someStr2 =
                    "412tgieoargj4a9349u2u-03jf3290rjf2390ja209fj1099u42n0f92qm93df3m-032jfq-102",
                someStr3 =
                    new string(Enumerable.Repeat('l', 512).ToArray()),
                IDK = Guid.Empty,
                NewProp = "wat"
            };
            _testObj_sealed = new TestSer_Sealed()
            {
                SomeValue = "idk"
            };
            _testObj_poly = new TestSer_Enclosed_Polymorphic_Object(){Value = testObj};
            _testObj_poly_inherited =
                new TestSer_Enclosed_Polymorphic_TestSerNoRef()
                    { SomeValue = testObj };
            _binaryRep_noRef = _poolSer.ToBinary(testObj);
            _binaryRep_withRef = _poolSer.ToBinary(testObj_WithRef);
            _binaryRep_manifest = _poolSer.IncludeManifest
                ? _poolSer.GetManifest(testObj) : string.Empty;
            _binaryRep_withRef_manifest = _poolSer.IncludeManifest?
                _poolSer.GetManifest(testObj_WithRef) : string.Empty;
            _binaryRep_inheriting = _poolSer.ToBinary(_testObj_inheriting);
            _binaryRep_inheriting_manifest = _poolSer.IncludeManifest?
                _poolSer.GetManifest(_testObj_inheriting) : string.Empty;
            _binaryRep_sealed = _poolSer.ToBinary(_testObj_sealed);
            _binaryRep_sealed_manifest = _poolSer.IncludeManifest?
                _poolSer.GetManifest(_testObj_sealed) : string.Empty;
            _binaryRep_poly = _poolSer.ToBinary(_testObj_poly);
            _binaryRep_poly_manifest = _poolSer.IncludeManifest?
                _poolSer.GetManifest(_testObj_poly) : string.Empty;
            _binaryRep_poly_inherited = _poolSer.ToBinary(_testObj_poly_inherited);
            _binaryRep_poly_inherited_manifest = _poolSer.IncludeManifest?
                _poolSer.GetManifest(_testObj_poly_inherited) : string.Empty;
        }

        protected abstract string testSysName { get; }

        protected abstract Func<ExtendedActorSystem,
                ImmutableHashSet<SerializerDetails>>
            createSerializer { get; }

        private static TestSer testObj = new()
        {
            Id = 124,
            someStr =
                "412tgieoargj4a9349u2u-03jf3290rjf2390ja209fj1099u42n0f92qm93df3m-032jfq-102",
            someStr2 =
                "412tgieoargj4a9349u2u-03jf3290rjf2390ja209fj1099u42n0f92qm93df3m-032jfq-102",
            someStr3 =
                new string(Enumerable.Repeat('l',512).ToArray()),
            IDK = Guid.Empty
        };

        private readonly TestSer_WithRef testObj_WithRef;

        private ActorSystem _sys_noPool;
        private Serializer _poolSer;
        private readonly TestSer_Inheriting _testObj_inheriting;
        private readonly TestSer_Sealed _testObj_sealed;
        private readonly string _binaryRep_sealed_manifest;
        private readonly string _binaryRep_inheriting_manifest;
        private readonly byte[] _binaryRep_sealed;
        private readonly byte[] _binaryRep_inheriting;
        private readonly string _binaryRep_withRef_manifest;
        private readonly string _binaryRep_manifest;
        private readonly byte[] _binaryRep_withRef;
        private readonly byte[] _binaryRep_noRef;
        private readonly TestSer_Enclosed_Polymorphic_Object _testObj_poly;
        private readonly TestSer_Enclosed_Polymorphic_TestSerNoRef _testObj_poly_inherited;
        private readonly byte[] _binaryRep_poly;
        private readonly string _binaryRep_poly_manifest;
        private readonly byte[] _binaryRep_poly_inherited;
        private readonly string _binaryRep_poly_inherited_manifest;

        private const int _numIters = 1000; 
        
        private void Serialize(int wat, bool parallel = false)
        {
            if (parallel)
            {
                for (int i = 0; i < _numIters; i++)
                {
                    DoSerInternal(wat);
                }
            }
            else
            {
                DoSerInternal(wat);
            }
        }

        private void DoSerInternal(int wat)
        {
            switch (wat)
            {
                case 0:
                    _poolSer.ToBinary(testObj);
                    break;
                case 1:
                    _poolSer.ToBinary(testObj_WithRef);
                    break;
                case 2:
                    _poolSer.ToBinary(_testObj_inheriting);
                    break;
                case 3:
                    _poolSer.ToBinary(_testObj_sealed);
                    break;
                case 4:
                    _poolSer.ToBinary(_testObj_poly);
                    break;
                case 5:
                    _poolSer.ToBinary(_testObj_poly_inherited);
                    break;
            }
        }

        private void DeSerialize(int wat, bool parallel = false)
        {
            byte[] _bR = null;
            string manifest = string.Empty;
            Type _t = default;
            switch (wat)
            {
                case 0:
                    _bR = _binaryRep_noRef;
                    manifest = _binaryRep_manifest;
                    _t = typeof(TestSer);
                    break;
                case 1:
                    _bR = _binaryRep_withRef;
                    manifest = _binaryRep_withRef_manifest;
                    _t = typeof(TestSer_WithRef);
                    break;
                case 2:
                    _bR = _binaryRep_inheriting;
                    manifest = _binaryRep_inheriting_manifest;
                    _t = typeof(TestSer_Inheriting);
                    break;
                case 3:
                    _bR = _binaryRep_sealed;
                    manifest = _binaryRep_sealed_manifest;
                    _t = typeof(TestSer_Sealed);
                    break;
                case 4:
                    _bR = _binaryRep_poly;
                    manifest = _binaryRep_poly_manifest;
                    _t = typeof(TestSer_Enclosed_Polymorphic_Object);
                    break;
                case 5:
                    _bR = _binaryRep_poly_inherited;
                    manifest = _binaryRep_poly_inherited_manifest;
                    _t = typeof(TestSer_Enclosed_Polymorphic_TestSerNoRef);
                    break;
            }

            if (parallel)
            {
                for (int i = 0; i < _numIters; i++)
                {
                    DoDeserInternal(_bR, manifest, _t);
                }
            }
            else
            {
                DoDeserInternal(_bR,manifest,_t);
            }
        }

        private void DoDeserInternal(byte[]? _bR, string manifest, Type? _t)
        {
            if (_poolSer.IncludeManifest && _poolSer is SerializerWithStringManifest swsm)
            {
                swsm.FromBinary(_bR, manifest);
            }
            else
            {
                _poolSer.FromBinary(_bR,
                    _t);
            }
        }

        [Benchmark]
        public void Serialize_SimpleObj_NonSealed_NoRef()
        {
            Serialize(0);
        }
        [Benchmark]
        public void Serialize_SimpleObj_NonSealed_WithRef()
        {
            Serialize(1);
        }
        
        [Benchmark]
        public void Serialize_SimpleObj_Inheriting_NoRef()
        {
            Serialize(2);
        }
        
        [Benchmark]
        public void Serialize_SimpleObj_Sealed_NoRef()
        {
            Serialize(3);
        }
        
        [Benchmark]
        public void SerializePoly_Object_NoRef()
        {
            Serialize(4);
        }
        [Benchmark]
        public void SerializePoly_Inheriting_NoRef()
        {
            Serialize(5);
        }
        [Benchmark]
        public void Serialize_MultiTask_SimpleObj_NonSealed_NoRef()
        {
            MultiTasks_Do(0);
        }
        [Benchmark]
        public void Serialize_MultiTask_SimpleObj_NonSealed_WithRef()
        {
            MultiTasks_Do(1);
        }
        
        [Benchmark]
        public void Serialize_MultiTask_SimpleObj_Inheriting_NoRef()
        {
            MultiTasks_Do(2);
        }
        
        [Benchmark]
        public void Serialize_MultiTask_SimpleObj_Sealed_NoRef()
        {
            MultiTasks_Do(3);
        }
        [Benchmark]
        public void Serialize_MultiTask_Poly_Object_NoRef()
        {
            MultiTasks_Do(4);
        }
        [Benchmark]
        public void Serialize_MultiTask_Poly_Inheriting_NoRef()
        {
            MultiTasks_Do(5);
        }
        private void MultiTasks_Do(int wat)
        {
            Task.WaitAll(Enumerable.Repeat(wat,10)
                .Select((i) => Task.Run(()=>Serialize(i,true))).ToArray());
        }
        
        [Benchmark]
        public void DeSerialize_SimpleObj_NonSealed_NoRef()
        {
            DeSerialize(0);
        }
        [Benchmark]
        public void DeSerialize_SimpleObj_NonSealed_WithRef()
        {
            DeSerialize(1);
        }
        
        [Benchmark]
        public void DeSerialize_SimpleObj_Inheriting_NoRef()
        {
            DeSerialize(2);
        }
        
        [Benchmark]
        public void DeSerialize_SimpleObj_Sealed_NoRef()
        {
            DeSerialize(3);
        }
        
        [Benchmark]
        public void DeSerialize_Poly_Object_NoRef()
        {
            DeSerialize(4);
        }
        [Benchmark]
        public void DeSerialize_Poly_Inherited_NoRef()
        {
            DeSerialize(5);
        }
        [Benchmark]
        public void DeSerialize_MultiTask_SimpleObj_NonSealed_NoRef()
        {
            DeserializeMultiTasks_Do(0);
        }
        [Benchmark]
        public void DeSerialize_MultiTask_SimpleObj_NonSealed_WithRef()
        {
            DeserializeMultiTasks_Do(1);
        }
        
        [Benchmark]
        public void DeSerialize_MultiTask_SimpleObj_Inheriting_NoRef()
        {
            DeserializeMultiTasks_Do(2);
        }
        
        [Benchmark]
        public void DeSerialize_MultiTask_SimpleObj_Sealed_NoRef()
        {
            DeserializeMultiTasks_Do(3);
        }
        private void DeserializeMultiTasks_Do(int wat)
        {
            Task.WaitAll(Enumerable.Repeat(wat,10)
                .Select((i) => Task.Run(()=>DeSerialize(i,true))).ToArray());
        }
    }
}
